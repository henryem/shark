package blinkdb.util

import java.util.Random

object MathUtils {
  def quantile[I](quantile: Double)(data: Seq[I])(implicit ord: Ordering[I]): I = {
    require(0 <= quantile && quantile <= 1)
    val rank = math.min(data.size-1, math.round(quantile*data.size).toInt)
    quickSelect(rank, data, new Random(012)) //FIXME
  }
  
  def quickSelect[I](rank: Int, data: Seq[I], random: Random)(implicit ord: Ordering[I]): I = {
    val dataSize = data.size
    require(0 <= rank && rank < dataSize, "rank %d outside data range".format(data.size))
    val pivot = data(random.nextInt(dataSize))
    val smaller = data.filter(datum => ord.lt(datum, pivot))
    val larger = data.filter(datum => ord.gt(datum, pivot))
    val smallerSize = smaller.size
    val largerSize = larger.size
    val tiedSize = dataSize - smallerSize - largerSize
    if (smallerSize > rank) {
      quickSelect(rank, smaller, random)
    } else if (smallerSize + tiedSize <= rank) {
      quickSelect(rank - smallerSize - tiedSize, larger, random)
    } else {
      require(tiedSize > 0)
      require(smallerSize + tiedSize >= rank)
      pivot
    }
  }
}