(ns metabase.mcp.metrics-test
  (:require
   [clojure.test :refer :all]
   [metabase.lib-be.core :as lib-be]
   [metabase.lib.core :as lib]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.mcp.metrics :as mcp.metrics]
   [metabase.test :as mt]
   [metabase.test.fixtures :as fixtures]
   [metabase.util.json :as json]
   [toucan2.core :as t2]))

(set! *warn-on-reflection* true)

(use-fixtures :once (fixtures/initialize :db :test-users))

(defn- mp []
  (lib-be/application-database-metadata-provider (mt/id)))

(defn- json-roundtrip
  "MCP tool results are JSON that gets decoded with keyword keys; mimic that so the metrics code sees the same
   shapes (string enum values, keyword keys) it sees in production."
  [x]
  (-> x json/encode json/decode+kw))

(deftest query-table-ids-mbql-test
  (let [mp (mp)]
    (testing "source table only"
      (is (= #{(mt/id :orders)}
             (mcp.metrics/query-table-ids mp (lib/query mp (mt/mbql-query orders {:limit 1}))))))
    (testing "explicit join adds the joined table"
      (is (= #{(mt/id :orders) (mt/id :products)}
             (mcp.metrics/query-table-ids
              mp
              (lib/query mp (mt/mbql-query orders
                              {:joins [{:source-table $$products
                                        :alias        "p"
                                        :condition    [:= $product_id &p.products.id]}]}))))))
    (testing "implicit FK join adds the parent table"
      (is (= #{(mt/id :orders) (mt/id :products)}
             (mcp.metrics/query-table-ids
              mp
              (lib/query mp (mt/mbql-query orders {:fields [$id $product_id->products.category]}))))))
    (testing "a JSON-roundtripped MBQL 5 query (as found on a QP result's json_query) still resolves"
      (let [serialized (-> (lib/query mp (mt/mbql-query orders {:limit 1}))
                           lib/prepare-for-serialization
                           json-roundtrip)]
        (is (= #{(mt/id :orders)}
               (mcp.metrics/query-table-ids mp (lib/query mp serialized))))))))

(deftest query-table-ids-native-test
  (let [mp (mp)]
    (testing "tables are parsed out of native SQL"
      (is (= #{(mt/id :orders) (mt/id :products)}
             (mcp.metrics/query-table-ids
              mp
              (lib/query mp (mt/native-query {:query "SELECT o.id FROM ORDERS o JOIN PRODUCTS p ON o.product_id = p.id"}))))))
    (testing "a legacy-shaped native json_query (string :type, as returned by execute_sql) still resolves"
      (let [serialized (json-roundtrip {:database (mt/id)
                                        :type     :native
                                        :native   {:query "SELECT * FROM VENUES"}})]
        (is (= #{(mt/id :venues)}
               (mcp.metrics/query-table-ids mp (lib/query mp serialized))))))))

(deftest query-table-ids-cards-test
  (let [mp (mp)]
    (testing "tables underneath a referenced card are included"
      (mt/with-temp [:model/Card card {:dataset_query (mt/mbql-query products)}]
        (let [q (lib/query mp (lib.metadata/card mp (:id card)))]
          (is (= #{(mt/id :products)}
                 (mcp.metrics/query-table-ids mp q))))))
    (testing "cards are followed transitively, and a native card is parsed"
      (mt/with-temp [:model/Card inner {:dataset_query (mt/native-query {:query "SELECT * FROM CHECKINS"})}
                     :model/Card outer {:dataset_query (mt/mbql-query nil {:source-table (str "card__" (:id inner))})}]
        (let [q (lib/query mp (lib.metadata/card mp (:id outer)))]
          (is (= #{(mt/id :checkins)}
                 (mcp.metrics/query-table-ids mp q))))))))

(deftest record-query-result-test
  (testing "a QP result map (as returned by the query tools) is turned into query, duration, row and table metrics"
    (mt/with-prometheus-system! [_ system]
      (let [db-name (t2/select-one-fn :name :model/Database (mt/id))
            user-id (mt/user->id :rasta)
            result  (json-roundtrip
                     (mt/with-current-user user-id
                       (mt/user-http-request :rasta :post 202 "dataset" (mt/mbql-query orders {:limit 3}))))]
        (is (= "completed" (:status result)))
        (mt/with-current-user user-id
          (mcp.metrics/record-query-result! "execute_query" result))
        (is (== 1 (mt/metric-value system :metabase-mcp/queries
                                   {:tool       "execute_query"
                                    :database   db-name
                                    :query-type "mbql"
                                    :status     "completed"
                                    :user-id    (str user-id)})))
        (is (== 1 (:count (mt/metric-value system :metabase-mcp/query-duration-ms
                                           {:tool "execute_query" :database db-name}))))
        (is (== 3 (:sum (mt/metric-value system :metabase-mcp/query-rows {:tool "execute_query"}))))
        (is (== 1 (mt/metric-value system :metabase-mcp/query-tables
                                   {:database db-name :schema "PUBLIC" :table "ORDERS" :user-id (str user-id)}))))))
  (testing "garbage input never throws"
    (mt/with-prometheus-system! [_ _system]
      (is (nil? (mcp.metrics/record-query-result! "query" nil)))
      (is (nil? (mcp.metrics/record-query-result! "query" {:database_id "not-an-id"})))
      (is (nil? (mcp.metrics/record-query-result! "query" {:database_id (mt/id) :json_query {:lib/type "nonsense"}}))))))
