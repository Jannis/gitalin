(ns gitalin.transit
  (:require [cognitect.transit :as transit])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))

(defn transit-write
  [data]
  (let [stream (ByteArrayOutputStream. 4096)]
    (-> stream
        (transit/writer :json)
        (transit/write data))
    (.toString stream)))

(defn transit-read
  [data]
  (let [stream (ByteArrayInputStream. data)]
    (-> stream
        (transit/reader :json)
        (transit/read))))
