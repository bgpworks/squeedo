(ns com.climate.squeedo.async-util-test
  (:require [com.climate.squeedo.async-util :as sut]
            [clojure.test :as t]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as impl]
            [com.climate.squeedo.test-utils :refer [with-timeout]]))

(t/deftest test-auto-closing-chan
  (let [chan (sut/auto-closing-chan)]
    (t/is (false? (impl/closed? chan)))
    (with-timeout 1000
      (async/>!! chan 1))
    (t/is (true? (impl/closed? chan)))))

(t/deftest test-wait-all-close
  (t/testing "Empty seq"
    (let [close-ch (sut/wait-all-close [])]
      (with-timeout 1000
        ;; wait until async's dispatch threads do their works
        (async/<!! close-ch))
      (t/is (true? (impl/closed? close-ch)))))

  (t/testing "One channel"
    (let [chan (async/chan)
          close-ch (sut/wait-all-close [chan])]
      (t/is (false? (impl/closed? close-ch)))
      (with-timeout 1000
        (async/>!! chan 1))
      (t/is (false? (impl/closed? close-ch)))
      (with-timeout 1000
        (async/>!! chan 1))
      (t/is (false? (impl/closed? close-ch)))
      (async/close! chan)
      (with-timeout 1000
        (async/<!! close-ch))
      (t/is (true? (impl/closed? close-ch)))))

  (t/testing "100 channels"
    (let [chans (repeatedly 100
                            (fn [] (async/chan)))
          close-ch (sut/wait-all-close chans)]
      (t/is (false? (impl/closed? close-ch)))
      (with-timeout 1000
        (doseq [chan chans]
          (async/>!! chan 1)))
      (t/is (false? (impl/closed? close-ch)))
      (with-timeout 1000
        (doseq [chan chans]
          (async/>!! chan 1)))
      (t/is (false? (impl/closed? close-ch)))
      (doseq [chan chans]
        (async/close! chan))
      (with-timeout 1000
        (async/<!! close-ch))
      (t/is (true? (impl/closed? close-ch))))))


(t/deftest test-pipes
  (t/testing "All elements are transfered."
    (let [out-ch (async/chan)
          chan-count 10
          val-count 100
          values (repeatedly (* chan-count val-count)
                             gensym)
          from-ch-ch (->> (partition-all val-count
                                         values)
                          (mapv async/to-chan)
                          async/to-chan)]
      (sut/pipes from-ch-ch out-ch)
      (let [result (with-timeout 1000
                     (async/<!! (async/into [] out-ch)))]
        (t/is (= (sort values) (sort result))))))

  (t/testing "A long running channel should not block out-ch"
    (let [fast1-elem [1 2 3]
          chan-fast1 (async/to-chan fast1-elem)
          chan-slow (async/chan)
          fast2-elem [4 5 6]
          chan-fast2 (async/to-chan fast2-elem)
          from-ch-ch (async/to-chan [chan-fast1 chan-slow chan-fast2])
          out-ch (async/chan)]
      (sut/pipes from-ch-ch out-ch)
      (with-timeout 1000
        (dotimes [_ (+ (count fast1-elem)
                       (count fast2-elem))]
          (async/<!! out-ch)))
      (t/is (false? (impl/closed? out-ch)))
      (with-timeout 1000
        (async/>!! chan-slow 1)
        (async/close! chan-slow)
        (async/<!! out-ch)
        ;; wait for close
        (async/<!! out-ch))
      (t/is (true? (impl/closed? out-ch))))))
