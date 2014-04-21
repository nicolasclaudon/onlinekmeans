/**
 * Created by nico on 20/04/2014.
 */

package opensource.OnlineKMeans
import org.apache.spark.mllib.clustering.KMeansModel

class OnlineKMeansModel (override val clusterCenters: Array[Array[Double]],
                         val weights: Array[Int] = Array(1)) extends KMeansModel(clusterCenters){

}
