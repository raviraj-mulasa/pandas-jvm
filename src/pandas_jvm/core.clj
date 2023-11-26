(ns pandas-jvm.core
  (:gen-class))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "I'm a little teapot!"))


"
Clojure is a JVM language , predominantly a functional programming language and a dialect of Lisp.
Clojure provides not only easy access to the Java frameworks but also provides rich set of immutable, persistent data structures.

https://github.com/techascent/tech.ml.dataset
In this article , we will explore tech.ml.dataset (TMD) a Clojure library for tabular data processing.
This library is analogous to Python's Pandas, or R's data.table.
TMD dataset is similar to Pandas Dataframe.

Our example based approach will explore the following
1. Creating dataset from a CSV file
2. Creating dateset from Maps
3. Filtering data - Projection and Selection
4. GroupBy / Aggregation
5. Dataset Manipulation - Add a column
6. Pandas-style Merge - Joins
"



(require '[tech.v3.dataset :as  ds])
(require '[tech.v3.dataset.join :as ds-join])
(require '[tech.v3.datatype.functional :as dtype-func])
(require '[tech.v3.datatype.datetime.operations :as dtype-dt-ops])

"
1. Creating dataset from a CSV file
https://github.com/vega/datalib/blob/master/test/data/stocks.csv
"
(require '[tech.v3.dataset :as  ds])

(def stocks (ds/->dataset "stocks.csv" {:key-fn keyword}))
(ds/column-names pandas-jvm.core/stocks)
(type pandas-jvm.core/stocks)
(get pandas-jvm.core/stocks :date)
(meta (get pandas-jvm.core/stocks :date))


(ds/head pandas-jvm.core/stocks)
(ds/tail pandas-jvm.core/stocks)
(ds/head pandas-jvm.core/stocks 10)
(ds/tail pandas-jvm.core/stocks 10)

"
2. Dataset Manipulation - Add a column
"
(require '[tech.v3.datatype.datetime.operations :as dtype-dt-ops])
(def stocks
  (ds/row-map pandas-jvm.core/stocks
              (fn [row]
                {:symbol (:symbol row)
                 :year (dtype-dt-ops/long-temporal-field :years (:date row))
                 }
                )
              )
  )
(ds/column-names pandas-jvm.core/stocks)
(ds/head pandas-jvm.core/stocks)
(ds/tail pandas-jvm.core/stocks)


;(def stocks (ds/add-or-update-column pandas-jvm.core/stocks :epoch (dtype-dt-ops/datetime->epoch (pandas-jvm.core/stocks :date))))
;(meta (get pandas-jvm.core/stocks :epoch))
;(def stocks (ds/add-or-update-column pandas-jvm.core/stocks :year (dtype-dt-ops/long-temporal-field :years (pandas-jvm.core/stocks :date))))
;(meta (get pandas-jvm.core/stocks :year))
;(ds/column-names pandas-jvm.core/stocks)


;(ds/row-map pandas-jvm.core/stocks (fn [row]
;                              {"symbol" (keyword (row "symbol"))
;                               :year (dtype-dt-ops/long-temporal-field :years (dtype-dt-ops/datetime->epoch (row "date"))}))
;
;(def stocks
;  (ds/row-map pandas-jvm.core/stocks
;              (fn [row]
;                {:symbol (:symbol row)
;                 :year (dtype-dt-ops/long-temporal-field :years (:date row))
;                 }))


"3. Filtering data - Projection"
(ds/select-columns pandas-jvm.core/stocks [:symbol, :date])


"3. Filtering data - Selection"
(ds/filter-column  pandas-jvm.core/stocks  :symbol #{"IBM"})
;(ds/filter  pandas-jvm.core/stocks  {:symbol #{"IBM"} :year == 2001} (and [:symbol #{"IBM"} :year == 2001]) )
(ds/filter-column pandas-jvm.core/stocks :price #(> % 100.0))

"4. GroupBy / Aggregation - Descriptive statistics"
(ds/descriptive-stats (ds/filter-column pandas-jvm.core/stocks :symbol #{"AAPL"}))


"4. GroupBy / Aggregation - Min price by Symbol"
(require '[tech.v3.datatype.functional :as dtype-func])
(->>
  (ds/group-by pandas-jvm.core/stocks :symbol)
  (vals)
  (map (fn [ds]
         {:symbol (first (ds :symbol))
          :date (first (ds :date))
          :min-price (dtype-func/reduce-min (ds :price))
          }
         )
       )
  (sort-by (juxt :min-price :date))
  (ds/->>dataset)
  )

"4. GroupBy / Aggregation - Max price by Symbol"
(->>
  (ds/group-by pandas-jvm.core/stocks :symbol)
  (vals)
  (map (fn [ds]
         {:symbol (first (ds :symbol))
          :date (first (ds :date))
          :max-price (dtype-func/reduce-max (ds :price))
          }
         )
       )
  (sort-by (juxt :max-price :date))
  (ds/->>dataset)
  )

"4. GroupBy / Aggregation - Avg price by Symbol"
(->>
  (ds/group-by pandas-jvm.core/stocks :symbol)
  (vals)
  (map (fn [ds]
         {:symbol (first (ds :symbol))
          :date (first (ds :date))
          :avg-price (dtype-func/mean (ds :price))
          }
         )
       )
  (sort-by (juxt :avg-price :date))
  (ds/->>dataset)
  )


"4. GroupBy / Aggregation - Max price by Year for a Symbol"
(->>
  (ds/group-by  (ds/filter-column  pandas-jvm.core/stocks  :symbol #{"AAPL"}) :year)
  (vals)
  (map (fn [ds]
         {:symbol (first (ds :symbol))
          :year (first (ds :year))
          :max-price (dtype-func/reduce-max (ds :price))}))
  (sort-by (juxt :symbol :year))
  (ds/->>dataset)
  )

"4. GroupBy / Aggregation - Min price by Year for a Symbol"
(->>
  (ds/group-by  (ds/filter-column  pandas-jvm.core/stocks  :symbol #{"AAPL"}) :year)
  (vals)
  (map (fn [ds]
         {:symbol (first (ds :symbol))
          :year (first (ds :year))
          :min-price (dtype-func/reduce-min (ds :price))}))
  (sort-by (juxt :symbol :year))
  (ds/->>dataset)
  )

"4. GroupBy / Aggregation - Avg price by Year for a Symbol"
(->>
  (ds/group-by (ds/filter-column  pandas-jvm.core/stocks  :symbol #{"AAPL"}) (juxt :symbol :year))
  (vals)
  (map (fn [ds]
         {:symbol (first (ds :symbol))
          :year (first (ds :year))
          :avg-price (dtype-func/mean (ds :price))}))
  (sort-by (juxt :symbol :year))
  (ds/->>dataset)
  )

"
5. Creating dateset from Maps
"
(def employee_dept (ds/->dataset {
                                  :employee ['Bob, 'Jake, 'Lisa, 'Sue]
                                  :group ['Accounting, 'Engineering, 'Engineering, 'HR]
                                  }
                                 )
  )

(def employee_hire_year (ds/->dataset {
                                       :employee ['Lisa, 'Bob, 'Jake, 'Sue]
                                       :hire_year [2004, 2008, 2012, 2014]
                                       }
                                      )
  )

(def employee_salary (ds/->dataset {
                                    :name ['Lisa, 'Bob, 'Jake, 'Sue]
                                    :salary [70000, 80000, 120000, 90000]
                                    }
                                   )
  )

(def dept_supervisor (ds/->dataset {
                                    :group ['Accounting, 'Engineering, 'HR]
                                    :supervisor ['Carly, 'Guido, 'Steve]
                                    }
                                   )
  )

(def dept_skills (ds/->dataset {
                                :group ['Accounting, 'Accounting, 'Engineering, 'Engineering, 'HR, 'HR],
                                :skills ['math, 'spreadsheets, 'coding, 'linux, 'spreadsheets, 'organization]}
                               )
  )




"
6. Pandas-style Merge - Joins
Pandas-style [merge](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html).
"
(require '[tech.v3.dataset.join :as ds-join])

(ds-join/pd-merge employee_dept dept_skills  {:how :cross } )
(ds-join/pd-merge employee_dept dept_skills  {:on [:group] } )

(def emp_hire_year_dept (ds-join/pd-merge employee_dept employee_hire_year {:on [:employee] :how :inner}))
(ds-join/pd-merge pandas-jvm.core/emp_hire_year_dept pandas-jvm.core/dept_supervisor {:on [:group] :how :inner})

(ds-join/pd-merge employee_dept employee_salary {:left-on [:employee] :right-on [:name]})