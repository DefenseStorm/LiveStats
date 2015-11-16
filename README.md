# LiveStats - Online Statistical Algorithms for Java

This is a reimplementation of http://github.com/cxxr/LiveStats in Java, with the additional feature of time or count-based decay.

LiveStats solves the problem of generating accurate statistics for when your data set is too large to fit in memory, or too costly to sort. Just add your data to the LiveStats object, and query the methods on it to produce statistical estimates of your data.

LiveStats doesn't keep any items in memory, only estimates of the statistics. This means you can calculate statistics on an arbitrary amount of data.

LiveStats requires Java 8.

## Example usage

See tests.

In addition to quantile calculations, LiveStats also provides other stats too, such as minimum, maximum, [kurtosis](http://en.wikipedia.org/wiki/Kurtosis), and [skewness](http://en.wikipedia.org/wiki/Skewness).

# FAQ

## How does this work? 
LiveStats uses the [P-Square Algorithm for Dynamic Calculation of Quantiles and Histograms without Storing Observations](http://www.cs.wustl.edu/~jain/papers/ftp/psqr.pdf) and other online statistical algorithms. 

## How accurate is it?

Very accurate. Check out the tests for details.
