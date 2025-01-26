# Spark Sample

手元で Spark を動かすサンプル


## 必要な環境

* Scala (sbt)
* Spark

どちらも SDKMAN で入ります。

Scala のバージョンは 2.12 系か 2.13 系で。
最初に作ったら Scala3 になってて上手く動きませんでした。

それから、 CSV を用意します。
どこでも良いのですが、慣例に従って resources ディレクトリの中に入れました。
今回は以下の3つを用意しました。

* users.csv
* langs.csv
* user_langs.csv

`Main.scala` を 説明すると、 spark.read.csv メソッドで CSV を読み込みます。今回はヘッダ付きなので解釈できるようにオプションを入れています。

このメソッドの返り値は sql.DataFrame 型で、これは実体としてはたしか DataSet[Row] 型とかだったかな？ （編集済み）

show メソッドで内容を表示させることができます。複数台ワーカーがあったらワーカー毎に表示とかだったような気がする。

join メソッドで SQL の JOIN 相当の処理を行うことができます。カラムの指定は見ての通りのやり方。 joinType （ InnerJoin とか LeftJoin とか）の指定もできます。

で、結合した結果を show して終わり。もちろん、必要部分だけ SELECT したりフィルターしたりもできる。窓関数も使えます。

最後は spark.stop で止めて終わり。


## これを動かすには

まず jar ファイルを生成します。

```
sbt package
```

で、 jar ファイルが生成されるので、それを spark-submit コマンドに渡してあげます。

```
spark-submit --class=jp.cedretaber.minispark.Main --master="local[*]" target/scala-2.13/minispark_2.13-0.1.0-SNAPSHOT.jar
```

これで、こんな感じの出力が出てくるはず（他にもいっぱい出てくるけど）。

```
+---+----+-------+-------+---+-------+
| id|name|user_id|lang_id| id|   name|
+---+----+-------+-------+---+-------+
|  1|   a|      1|      1|  1|  ocaml|
|  1|   a|      1|      2|  2|haskell|
|  2|   b|      2|      3|  3|    sml|
|  3|   c|      3|      4|  4|  scala|
|  3|   c|      3|      5|  5|clojure|
|  4|   d|      4|      1|  1|  ocaml|
|  4|   d|      4|      5|  5|clojure|
+---+----+-------+-------+---+-------+
```
