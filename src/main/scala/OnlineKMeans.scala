/**
 * Created by nico on 20/04/2014.
 * Based on the work of freeman-lab
 * https://gist.github.com/freeman-lab/9672685
 */
package opensource.OnlineKMeans

import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import scala.util.Random.nextDouble
import scala.util.Random.nextGaussian
import java.util.Properties
import java.io.FileInputStream

class OnlineKMeans(
                    var k: Int,
                    var d: Int,
                    var maxIterations: Int,
                    var initializationMode: String) extends Serializable with Logging {

  private type ClusterCentersAndCounts = Array[(Array[Double], Int)]

  /**
   * Default constructor
   */
  def this() = this(2, 2, 1, "gauss")

  /**
   * Number of cluster to classify vectors.
   */
  def setK(k: Int): OnlineKMeans = {
    this.k = k
    this
  }

  /**
   * Vector Dimension. This cannot be changed during the clustering.
   * @param d
   * @return
   */
  def setD(d: Int): OnlineKMeans = {
    this.d = d
    this
  }

  /**
   * Number of iteration of the kmeans algorithm to converge centroids.
   */
  def setMaxIterations(maxIterations: Int): OnlineKMeans = {
    this.maxIterations = maxIterations
    this
  }

  /**
   * Set the initialization algorithm. Unlike batch KMeans, we
   * initialize randomly before we have seen any data. Options are "gauss"
   * for random Gaussian centers, and "pos" for random positive uniform centers.
   * Default: gauss
   */
  def setInitializationMode(initializationMode: String): OnlineKMeans = {
    if (initializationMode != "gauss" && initializationMode != "pos") {
      throw new IllegalArgumentException("Invalid initialization mode: " + initializationMode)
    }
    this.initializationMode = initializationMode
    this
  }

  /**
   * Initialize random points for KMeans clustering
   */
  def initRandom(): OnlineKMeansModel = {

    val clusters = new Array[(Array[Double], Int)](k)
    for (ik <- 0 until k) {
      clusters(ik) = initializationMode match {
        case "gauss" => (Array.fill(d)(nextGaussian()), 0)
        case "pos" => (Array.fill(d)(nextDouble()), 0)
      }
    }
    new OnlineKMeansModel(clusters.map(_._1), clusters.map(_._2))
  }

  /**
   * Takes two vector and add them.
   * This will sum each coordinates together.
   * All the x will be sum together, all the y will be sum together and so on.
   * @param v1 Vector 1 to be added
   * @param v2 Vector 2 to be added
   * @return the sum vector
   */
  def sumVectors(v1: Array[Double], v2: Array[Double]): Array[Double] = {
    // v1.zip(v2) will group each coordinates of a dimension inside a tuple
    v1.zip(v2).map {
      // Sum coordinates of a dimension
      // all x will be sum, all y will be sum and so on
      case (c1, c2) => c1 + c2
    }
  }

  /**
   * Ponderate a vector with a factor
   * @param v1
   * @param factor
   * @return
   */
  def ponderateVector(v1: Array[Double], factor: Int): Array[Double] = {
    v1.map(_ * factor)
  }

  /**
   * Sum the weights
   * @param w1
   * @param w2
   * @return
   */
  def sumWeights(w1: Int, w2: Int) = w1 + w2

  /**
   * KMeans algorithm run for each mini batch.
   * Data arrives per mini batch and is an collection of Vectors (Rdd)
   * Vectors are separate on k clusters. Distance is compute for each vector compared to all centroid.
   * A vector is assigned to a cluster based on the minimal distance to the centroid.
   * Once all vectors are assigned to a cluster we proceed to compute the intermediate centroid for each clusters.
   * Finally to get the new centroids we are going to compute for each computer the centroid of:
   *  - the previous iteration centroid
   *  - the intermediate centroid
   * In order to keep history the previous centroid as a weight which represents all the points that were used to compute it.
   * The new centroids are saved with their respective weight to an OnlineKMeansModel.
   * For simplicity the implementation of update do not compute the intermediateCentroid but only sum vectors of a cluster
   * which save us the computation to divide each coordinates by the weight
   * @param data Collection of vectors
   * @param model Centroids and Weights
   * @return
   */
  def update(data: RDD[Array[Double]], model: OnlineKMeansModel): OnlineKMeansModel = {

    val centroids = model.clusterCenters
    val weights = model.weights

    // do iterative KMeans updates on a batch of data
    for (i <- Range(0, maxIterations)) {
      // find nearest cluster to each point
      val nearest = data.map(point => (model.predict(point), (point, 1)))
      // get sums and weights for updating each cluster
      val vectorsMap = nearest.reduceByKey {
        case ((v1, w1), (v2, w2)) => (sumVectors(v1, v2), sumWeights(w1, w2))
      }.collectAsMap()
      // Find the new centroid for each cluster
      vectorsMap.foreach {
        case (key, value) => {
          val k = key
          val intermediateVector = value._1
          val intermediateWeight = value._2
          val oldWeight = model.weights(k)
          val oldCentroid = model.clusterCenters(k)
          val newWeight = intermediateWeight + oldWeight

          val newCentroid = sumVectors(intermediateVector,
            ponderateVector(oldCentroid, oldWeight)).map(_ / newWeight)
          weights(k) = newWeight
          centroids(k) = newCentroid
        }
      }
      val model.clusterCenters = centroids
      val model.weights = weights
    }


    for (i <- Range(0, centroids.length)) {
      logInfo("##########################################################")
      logInfo("Cluster centroid " + centroids(i).mkString(", ") + s" weight: " + weights(i))
      logInfo("##########################################################")
    }
    new OnlineKMeansModel(centroids, weights)

  }

  /** Main streaming operation: initialize the KMeans model
    * and then update it based on new data from the stream.
   */
  /**
   * Initiate centroids and then run kmeans foreach mini batch
   * @param data
   * @return
   */
  def runStreaming(data: DStream[Array[Double]]): DStream[Int] = {
    var model = initRandom()
    data.foreachRDD(RDD => model = update(RDD, model))
    data.map(point => model.predict(point))
  }

}

/** Top-level methods for calling Streaming KMeans clustering. */
object OnlineKMeans {

  /**
   * Instantiate the KMeans with correct parameters and runs it.
   * @param input Input DStream of (Array[Double]) data points
   * @param k Number of clusters to estimate.
   * @param d Number of dimensions per data point.
   * @param maxIterations Maximum number of iterations per batch.
   * @param initializationMode Random initialization of cluster centers.
   * @return Output DStream of (Int) assignments of data points to clusters.
   */
  def trainStreaming(input: DStream[Array[Double]],
                     k: Int,
                     d: Int,
                     maxIterations: Int,
                     initializationMode: String)
  : DStream[Int] = {
    new OnlineKMeans().setK(k)
      .setD(d)
      .setMaxIterations(maxIterations)
      .setInitializationMode(initializationMode)
      .runStreaming(input)
  }

  /**
   * Main program
   * Get configuration from properties file and instantiate the streaming application
   * @param args properties file
   */
  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println("Usage: OnlineKMeans <configuration file>")
      System.exit(1)
    }

    val (master, name, batchTime, directory, numberCluster, dimension, maxIterations, initializationMode) =
      try {
        val prop = new Properties()
        prop.load(new FileInputStream(args(0)))
        (
          prop.getProperty("onlinekmeans.spark.master"),
          prop.getProperty("onlinekmeans.spark.appname"),
          prop.getProperty("onlinekmeans.spark.batch.time").toInt,
          prop.getProperty("onlinekmeans.data.sources.dir"),
          prop.getProperty("onlinekmeans.clustering.number").toInt,
          prop.getProperty("onlinekmeans.data.dimension").toInt,
          prop.getProperty("onlinekmeans.clustering.max.iterations").toInt,
          prop.getProperty("onlinekmeans.clustering.initialization.mode")
          )
      } catch {
        case e: Exception =>
          e.printStackTrace()
          sys.exit(1)
      }

    val conf = new SparkConf().setMaster(master).setAppName(name)

    if (!master.contains("local")) {
      conf.setSparkHome(System.getenv("SPARK_HOME"))
        .setJars(List("target/scala-2.10/onlinekmeans_2.10-1.0.jar"))
        .set("spark.executor.memory", "100M")
    }

    /** Create Streaming Context */
    val ssc = new StreamingContext(conf, Seconds(batchTime))

    /** Train KMeans model */
    val data = ssc.textFileStream(directory).map(x => x.split(' ').map(_.toDouble))
    OnlineKMeans.trainStreaming(data, numberCluster, dimension, maxIterations, initializationMode)
    ssc.start()
  }
}
