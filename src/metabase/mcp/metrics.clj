(ns metabase.mcp.metrics
  "Prometheus instrumentation for the MCP endpoint.

   Every metric lives under the `metabase_mcp_` prefix and is declared in [[metabase.analytics.prometheus]]. This
   namespace only *records* values via the dependency-free [[metabase.analytics-interface.core]] reporter, so it is
   a no-op when Prometheus is not configured (`MB_PROMETHEUS_SERVER_PORT` unset).

   What gets recorded, and where it is called from:

   - transport: HTTP responses by method/status/auth, JSON-RPC requests by method/status with duration, errors by
     JSON-RPC code, batch sizes, sessions created/deleted, active SSE streams ([[metabase.mcp.api]]).
   - tools: calls by tool/status/user, error reasons, duration ([[metabase.mcp.tools]]).
   - queries: for the tools that run a query (`query`, `execute_query`, `execute_sql`, `execute_question`), one
     observation per execution labelled by tool, database, query type and status, plus the QP running time, rows
     returned, and one increment per table (or MongoDB collection) the query touched — source tables, joins,
     implicit FK joins, template tags, and the tables underneath any card/model/metric the query references.

   Cardinality note: `user_id` is a label on `tool_calls`, `queries`, `query_tables` and `sessions_created`. That is
   deliberate — the point of these metrics is to see which people drive which load through MCP — but it means the
   series count grows with (users × tools) and (users × tables). Fine for a single company's instance; revisit before
   enabling on a large multi-tenant deployment. `metabase_mcp_user_info{user_id,email}` is an info gauge (always 1)
   so dashboards can join a user id to an email with `group_left`."
  (:require
   [clojure.set :as set]
   [metabase.analytics-interface.core :as analytics]
   [metabase.api.common :as api]
   [metabase.lib-be.core :as lib-be]
   [metabase.lib.core :as lib]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.sql-tools.core :as sql-tools]
   [metabase.util.log :as log]))

(set! *warn-on-reflection* true)

;;; ------------------------------------------------- Label helpers ------------------------------------------------

(defn- user-id-label
  "Stringified current user id, or `unknown` outside a user context (internal callers, tests)."
  []
  (str (or api/*current-user-id* "unknown")))

(defn- label-str
  "Coerce a label value to a non-nil string; nil becomes `unknown`."
  [x]
  (cond
    (nil? x)     "unknown"
    (keyword? x) (name x)
    :else        (str x)))

(defn- since-ms
  "Milliseconds elapsed since `start-ns` (a `System/nanoTime` reading)."
  [start-ns]
  (/ (- (System/nanoTime) start-ns) 1e6))

(defn- safely
  "Run `f`; log and swallow anything it throws. Metrics must never break a request."
  [what f]
  (try
    (f)
    (catch Throwable e
      (log/warnf e "MCP metrics: failed to record %s: %s" what (ex-message e)))))

;;; --------------------------------------------------- Transport ---------------------------------------------------

(defn record-http-response!
  "Count one HTTP response from the MCP handler. `auth` is `:session`, `:bearer` or `:none`."
  [request auth response]
  (safely "http response"
          #(analytics/inc! :metabase-mcp/requests
                           {:http-method (label-str (:request-method request))
                            :status      (label-str (:status response))
                            :auth        (label-str auth)})))

(defn record-http-error!
  "Count a request that escaped the handler via `raise`. Uses the `:status-code` from the exception data when
   present (the exceptions middleware will turn it into that status), otherwise 500."
  [request auth e]
  (record-http-response! request auth {:status (or (:status-code (ex-data e)) 500)}))

(defn wrap-respond
  "Return a `respond` fn that records the response before delegating to `respond`."
  [request auth respond]
  (fn [response]
    (record-http-response! request auth response)
    (respond response)))

(defn wrap-raise
  "Return a `raise` fn that records the failure before delegating to `raise`."
  [request auth raise]
  (fn [e]
    (record-http-error! request auth e)
    (raise e)))

(defn record-rpc!
  "Record one JSON-RPC message handled by `method`. `response` is the JSON-RPC response map, or nil for a
   notification. `start-ns` is the `System/nanoTime` reading taken before dispatch."
  [method response start-ns]
  (safely "rpc"
          (fn []
            (let [method (label-str method)
                  status (cond
                           (nil? response)   "notification"
                           (:error response) "error"
                           :else             "ok")]
              (analytics/inc! :metabase-mcp/rpc-requests {:rpc-method method :status status})
              (analytics/observe! :metabase-mcp/rpc-duration-ms {:rpc-method method} (since-ms start-ns))
              (when-let [code (get-in response [:error :code])]
                (analytics/inc! :metabase-mcp/rpc-errors {:rpc-method method :code (str code)}))))))

(defn timed-rpc
  "Call `(f)`, which must return a JSON-RPC response map (or nil for a notification), and record it under `method`."
  [method f]
  (let [start    (System/nanoTime)
        response (f)]
    (record-rpc! method response start)
    response))

(defn record-batch-size!
  "Observe the number of JSON-RPC messages carried by one POST."
  [n]
  (safely "batch size" #(analytics/observe! :metabase-mcp/batch-size n)))

(defn record-session-created!
  "Count a successful `initialize`, labelled by the client that connected."
  [client-info]
  (safely "session created"
          #(analytics/inc! :metabase-mcp/sessions-created
                           {:client-name    (label-str (:name client-info))
                            :client-version (label-str (:version client-info))
                            :user-id        (user-id-label)})))

(defn record-session-deleted!
  "Count a successful `DELETE` of a session."
  []
  (safely "session deleted" #(analytics/inc! :metabase-mcp/sessions-deleted)))

(defn sse-stream-opened!
  "Bump the active SSE stream gauge. Pair with [[sse-stream-closed!]] in a `finally`."
  []
  (safely "sse opened" #(analytics/inc! :metabase-mcp/sse-streams-active)))

(defn sse-stream-closed!
  "Decrement the active SSE stream gauge."
  []
  (safely "sse closed" #(analytics/dec-gauge! :metabase-mcp/sse-streams-active)))

(defn record-resource-read!
  "Count a `resources/read`. The URI is only used as a label when the read succeeded, so unknown client-supplied
   URIs cannot inflate the series count."
  [uri status]
  (safely "resource read"
          #(analytics/inc! :metabase-mcp/resource-reads
                           {:uri    (if (= status :ok) (label-str uri) "unknown")
                            :status (label-str status)})))

(defn record-user!
  "Set the `user_info` gauge for the current user so `user_id` labels elsewhere can be joined to an email."
  []
  (safely "user info"
          (fn []
            (when-let [user-id api/*current-user-id*]
              (when-let [email (:email @api/*current-user*)]
                (analytics/set-gauge! :metabase-mcp/user-info {:user-id (str user-id) :email email} 1))))))

;;; ----------------------------------------------------- Tools -----------------------------------------------------

(defn record-tool-error!
  "Count a failed tool call by `reason` (a short, bounded keyword such as `:unknown-tool`, `:scope-denied`,
   `:handle-not-found`, `:exception`, or `:api-<status>`)."
  [tool-name reason]
  (safely "tool error"
          #(analytics/inc! :metabase-mcp/tool-call-errors {:tool (label-str tool-name) :reason (label-str reason)})))

(defn record-tool-call!
  "Record one completed `tools/call`: outcome (from `:isError` on the MCP result) and wall-clock duration."
  [tool-name result start-ns]
  (safely "tool call"
          (fn []
            (let [tool (label-str tool-name)]
              (analytics/inc! :metabase-mcp/tool-calls {:tool    tool
                                                        :status  (if (:isError result) "error" "ok")
                                                        :user-id (user-id-label)})
              (analytics/observe! :metabase-mcp/tool-call-duration-ms {:tool tool} (since-ms start-ns))))))

;;; ---------------------------------------------------- Queries ----------------------------------------------------

(def query-tools
  "Tools whose successful result is a query-processor result map (`database_id`, `json_query`, `running_time`,
   `row_count`, `status`, ...). [[record-query-result!]] is called for these."
  #{"query" "execute_query" "execute_sql" "execute_question"})

(defn- extraction-error!
  [reason e]
  (log/debugf e "MCP metrics: table extraction failed (%s): %s" reason (ex-message e))
  (analytics/inc! :metabase-mcp/query-table-extraction-errors {:reason (label-str reason)}))

(defn- ->lib-query
  "Build a lib query (with metadata provider attached) from a serialized `json_query`. Handles both MBQL 5 and
   legacy-shaped native queries, as found on QP results. Returns nil (and counts an extraction error) on failure."
  [mp json-query]
  (when (map? json-query)
    (try
      (lib/query mp json-query)
      (catch Throwable e
        (extraction-error! :query-build e)
        nil))))

(defn- mongo-collection-table-ids
  "For a native query against a document store, the stage carries the `:collection` name instead of SQL to parse.
   Resolve it to a table id by name within the query's database."
  [mp query]
  (when-let [collection (get-in query [:stages 0 :collection])]
    (into #{}
          (comp (filter #(= (:name %) collection))
                (map :id))
          (lib.metadata/tables mp))))

(defn- native-table-ids
  "Table ids referenced by a native query: the MongoDB `:collection`, or whatever the SQL parser finds."
  [mp query]
  (or (not-empty (mongo-collection-table-ids mp query))
      (try
        (let [engine (:engine (lib.metadata/database mp))]
          (into #{} (keep :table) (sql-tools/referenced-tables engine query)))
        (catch Throwable e
          (extraction-error! :native-parse e)
          #{}))))

(defn- query-entity-ids
  [query]
  (try
    (lib/all-referenced-entity-ids [query])
    (catch Throwable e
      (extraction-error! :query-walk e)
      nil)))

(defn- card-underlying-query
  [mp card-id]
  (try
    (when-let [card (lib.metadata/card mp card-id)]
      (lib/card->underlying-query mp card))
    (catch Throwable e
      (extraction-error! :card-lookup e)
      nil)))

(defn query-table-ids
  "Return the set of table ids a lib `query` touches: source tables, explicit and implicit joins, template-tag
   tables, tables parsed out of native SQL (or the MongoDB collection), and — recursively — everything underneath
   any card, model or metric the query references. Visited cards are tracked to break cycles."
  [mp query]
  (loop [queue   [query]
         visited #{}
         tables  #{}]
    (if-let [q (first queue)]
      (let [ids       (query-entity-ids q)
            native    (if (lib/native? q) (native-table-ids mp q) #{})
            card-ids  (set/difference (set/union (:card ids) (:metric ids)) visited)
            sub-query (keep #(card-underlying-query mp %) card-ids)]
        (recur (into (subvec queue 1) sub-query)
               (into visited card-ids)
               (-> tables (into (:table ids)) (into native))))
      tables)))

(defn- table-labels
  [mp db-name table-id]
  (let [table (try (lib.metadata/table mp table-id) (catch Throwable _ nil))]
    {:database db-name
     :schema   (or (:schema table) "")
     :table    (or (:name table) (str table-id))
     :user-id  (user-id-label)}))

(defn record-query-result!
  "Record the metrics for one query run by `tool-name` from its QP `result` map (the decoded JSON the tool returned).
   Runs synchronously; metadata lookups are served from the request's metadata-provider cache and native SQL
   parsing is bounded by the sql-tools parser, so the added latency is small next to the query itself."
  [tool-name result]
  (safely "query result"
          (fn []
            (when (map? result)
              (let [{:keys [database_id json_query running_time row_count status]} result
                    db-id (or database_id (:database json_query))]
                (when (int? db-id)
                  (lib-be/with-metadata-provider-cache
                    (let [mp         (lib-be/application-database-metadata-provider db-id)
                          db-name    (or (try (:name (lib.metadata/database mp)) (catch Throwable _ nil))
                                         (str db-id))
                          query      (->lib-query mp json_query)
                          tool       (label-str tool-name)
                          query-type (cond
                                       (nil? query)       "unknown"
                                       (lib/native? query) "native"
                                       :else               "mbql")]
                      (analytics/inc! :metabase-mcp/queries {:tool       tool
                                                             :database   db-name
                                                             :query-type query-type
                                                             :status     (label-str status)
                                                             :user-id    (user-id-label)})
                      (when (number? running_time)
                        (analytics/observe! :metabase-mcp/query-duration-ms {:tool tool :database db-name} running_time))
                      (when (number? row_count)
                        (analytics/observe! :metabase-mcp/query-rows {:tool tool} row_count))
                      (when query
                        (doseq [table-id (query-table-ids mp query)]
                          (analytics/inc! :metabase-mcp/query-tables (table-labels mp db-name table-id))))))))))))
