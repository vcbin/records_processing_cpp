## description
使用C++编写的计算用户上次观看日期的命令行工具。能够充分利用多核CPU的计算能力以及C++11的新特性(std::async/std::move)，在8核服务器(10.200.8.72)上计算一亿条用户观看数据的上次观看日期时比MySQL和PostgreSQL快10倍以上,内存占用只有实现相同功能的AWK脚本的1/3
  1. 命令行工具gen_first_visit_day用于处理从数据库中导出的观看记录并在最后添加上次观看日期列，输入的格式为
```
		291976  "i99000693160305"       20160601
		292267  "i868029025619115"      20160601
		159041  "ia1000043130b26"       20160601
		166217  "ia10000480eea2f"       20160601
```
第1列为设备did,第2列为节目pid,第3列为观看记录日期,处理后的记录格式为
```
		291976  "i99000693160305"       20160601	20160601
		292267  "i868029025619115"      20160602	20160602
		159041  "ia1000043130b26"       20160603	20160601
		166217  "ia10000480eea2f"       20160604	20160601
```
其中，第3列和第4列相等的列即为该日期当天新增观看记录,由于输入输出文件都较大(2G+),因此repo里面没有提供完整的输入文件，72服务器上具体的输入文件地址为:
```
		 ll -t /data/rstudio2/vid_data/in/*.csv
```
gen_first_visit_day程序运行过程示例输出(--verbose)
```
key column indexs
0       1
file 'in/uv_fact_day_20160601-07.csv' line number:      86253886
thread number = 8
file splitting  time taken..
4.23475 s

stat file chunks minimum        time taken..
110.649 s
merging file chunks     time taken..
127.504 s
min dict parallel building      time taken..
282.431 s

min dict size:  58976142

allocated size for string output: 4021.45 MB

file parallel joining time      taken..
106.666 s

result file writing time        taken..
2.324e-06 s

'out/in/uv_fact_day_20160601-07_day2_final_res.csv'

```

  2. 命令行工具merge_pg_records用于合并从PostgreSQL数据库中导出的多个观看记录文件,并更新第3列的观看记录数组,添加最后一列上次观看日期，输入以"|"分隔字段，具体格式为
	文件1
```
			ffffe684-066c-4f6b-acae-af152243f3b4|43645|{160613}
			FFFFE7BD-148F-4BC4-630D-4BE48F118E88|151068|{160613}
			ffffe7ec-27e0-4302-a0b9-6e5a87c905ec|104817|{160613}
			ffffea80-a493-48de-bd41-dde439f08da9|291337|{160613}
```

文件2
```
			fffff032-aba4-4401-b993-c5f41d526a1c|151068|{160614}
			fffff233-e0c7-4ef6-927c-86ffe55b323c|151068|{160614}
			fffff233-e0c7-4ef6-927c-86ffe55b323c|293174|{160614}
			fffff4d7-69ae-46bf-9f59-519a58b46ae5|291976|{160614}
```
第1列为cookie, 第二列为节目id，第3列为数组类型，记录该条观看记录的多个观看日期
合并后的结果文件格式为
```
			ffffe684-066c-4f6b-acae-af152243f3b4|43645|{160613}|160613
			FFFFE7BD-148F-4BC4-630D-4BE48F118E88|151068|{160613,160614}|160613
			ffffe7ec-27e0-4302-a0b9-6e5a87c905ec|104817|{160613}|160613
			ffffea80-a493-48de-bd41-dde439f08da9|291337|{160614}|160614
```
从结果格式可以看出来，该格式是1. 中的原始记录格式的"压缩"版本，较大程度地降低了文件的存储空间，但是由于不符合SQL范式，对于日常的SQL操作非常不便,因此采用了两个命令行工具分别保持计算结果的原始版本和压缩版本。
  merge_pg_records程序运行过程示例输出(--verbose)
```
file 'test1-test2.csv' line number:     12775394
thread number = 8
file splitting  time taken..
1.10955 s

stat file chunks        time taken..
16.849 s
merging file chunks     time taken..
29.9681 s
days dict parallel building     time taken..
53.665 s

days dict size: 11578663

result string allocation time   taken..
3.1754e-05 s
allocated size for string output: 684.07 MB
generaing all lines time        taken..
21.3346 s
file writing time       taken..
0.855419 s

'out/test1-test2_merge_res.csv'
```
## requirements
	gcc 4.8+ (C++ 11 support)
	boost library(1.47+)
	pthread library

## install
	make

## usage
	./gen_first_visit_day --help
	./merge_pg_records --help

## profiling
	 sudo perf record -ag  -F 99 -u `id -u` -- ./gen_first_visit_day --verbose -k1,2 -n3 in/uv_fact_day_20160601-07.csv
	 sudo perf script > out.perf
	  ~/FlameGraph/stackcollapse-perf.pl ./out.perf > ./out.folded
	  ~/FlameGraph/flamegraph.pl ./out.folded > gen_first_visit_day.svg


	 sudo perf record -ag  -F 99 -u `id -u` -- ./merge_pg_records --verbose -k1,2 -n3 in/test1-100k.csv in/test2-100k.csv
	 sudo perf script > out.perf
	  ~/FlameGraph/stackcollapse-perf.pl ./out.perf > ./out.folded
	  ~/FlameGraph/flamegraph.pl ./out.folded > merge_pg_records.svg

## todo
  通过上一步profiling分析'perf report'以及FlameGraph的输出，发现两个程序主要耗时的都是大量字符串的mallocate/deallocate,因此可以考虑以下两种方案解决：
  1. 使用其它内存效率更高的数据结构存储结果字符串,例如[rope](http://www.sgi.com/tech/stl/ropeimpl.html)
  2. 如果数据库中需要提取的每个字段都是固定大小的话，大量小对象的new/delete可以采用memory pool，例如[Memory Pool](http://www.codinglabs.net/tutorial_memory_pool.aspx)或者[boost pool](http://www.boost.org/doc/libs/1_61_0/libs/pool/doc/html/boost_pool/pool/pooling.html#boost_pool.pool.pooling.concepts)

