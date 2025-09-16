package com.reviews.analysis

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import scala.util.{Try, Success, Failure}
import scala.annotation.tailrec

/**
 * Main application for analyzing Amazon product reviews using functional programming
 * principles with Apache Spark.
 * 
 * This application demonstrates:
 * - Pure functional transformations
 * - Immutable data structures
 * - Function composition
 * - Custom combinators
 * - Tail recursion
 * - Functional error handling
 */
object AmazonReviewsAnalysis {

  // ============== CASE CLASSES (Immutable Data Structures) ==============
  
  /** Represents a raw Amazon review from JSON */
  case class Review(
    reviewerID: String,
    asin: String,           // Amazon Standard Identification Number
    reviewerName: Option[String],
    helpful: Array[Int],    // [helpful_votes, total_votes]
    reviewText: String,
    overall: Double,        // Rating (1-5)
    summary: String,
    unixReviewTime: Long
  )
  
  /** Processed review with additional computed fields */
  case class ProcessedReview(
    reviewerID: String,
    productID: String,
    rating: Double,
    helpfulnessRatio: Double,
    reviewLength: Int,
    sentiment: String,
    year: Int
  )
  
  /** Product statistics aggregate */
  case class ProductStats(
    productID: String,
    avgRating: Double,
    reviewCount: Long,
    avgHelpfulness: Double,
    dominantSentiment: String
  )
  
  /** Reviewer profile aggregate */
  case class ReviewerProfile(
    reviewerID: String,
    totalReviews: Long,
    avgRating: Double,
    avgReviewLength: Double,
    helpfulnessScore: Double
  )

  // ============== PURE FUNCTIONS (Functional Core) ==============
  
  /** 
   * Calculates helpfulness ratio from helpful votes array
   * Demonstrates pure function with pattern matching
   */
  val calculateHelpfulness: Array[Int] => Double = {
    case Array(helpful, total) if total > 0 => helpful.toDouble / total
    case _ => 0.0
  }
  
  /**
   * Determines sentiment based on rating
   * Demonstrates pattern matching with guards
   */
  val determineSentiment: Double => String = {
    case rating if rating >= 4.0 => "positive"
    case rating if rating >= 3.0 => "neutral"
    case _ => "negative"
  }
  
  /**
   * Extracts year from unix timestamp
   * Pure function for date processing
   */
  val extractYear: Long => Int = timestamp => {
    new java.util.Date(timestamp * 1000L).getYear + 1900
  }
  
  // ============== CUSTOM COMBINATOR ==============
  
  /**
   * Custom combinator that applies a transformation only if a predicate is satisfied.
   * This demonstrates a higher-order function that combines conditional logic
   */
  def whenThen[A](predicate: A => Boolean)(transform: A => A): A => A = 
    (input: A) => if (predicate(input)) transform(input) else input
  
  /**
   * Combinator for chaining multiple transformations.
   * Demonstrates function composition
   */
  def pipe[A, B, C](f: A => B, g: B => C): A => C = 
    (a: A) => g(f(a))
  
  // ============== CURRIED FUNCTIONS ==============
  
  /**
   * Curried function for filtering reviews by minimum rating.
   * Demonstrates partial application
   */
  def filterByMinRating(minRating: Double)(review: ProcessedReview): Boolean =
    review.rating >= minRating
  
  /**
   * Curried function for filtering by review length.
   * Can be partially applied for different length thresholds
   */
  def filterByMinLength(minLength: Int)(review: ProcessedReview): Boolean =
    review.reviewLength >= minLength
  
  // ============== TAIL-RECURSIVE FUNCTIONS ==============
  
  /**
   * Tail-recursive function to calculate percentile from sorted list
   * Used for statistical analysis of ratings
   */
  @tailrec
  def calculatePercentile(sortedValues: List[Double], percentile: Double): Double = {
    require(percentile >= 0 && percentile <= 100, "Percentile must be between 0 and 100")
    
    sortedValues match {
      case Nil => 0.0
      case single :: Nil => single
      case list =>
        val index = ((percentile / 100.0) * (list.length - 1)).toInt
        list(index)
    }
  }
  
  /**
   * Tail-recursive function to group consecutive reviews by same reviewer
   * Demonstrates accumulator pattern in tail recursion
   */
  @tailrec
  def groupConsecutiveReviews(
    reviews: List[ProcessedReview],
    acc: List[List[ProcessedReview]] = Nil
  ): List[List[ProcessedReview]] = reviews match {
    case Nil => acc.reverse
    case head :: tail =>
      acc match {
        case Nil => 
          groupConsecutiveReviews(tail, List(head) :: Nil)
        case currentGroup :: rest if currentGroup.head.reviewerID == head.reviewerID =>
          groupConsecutiveReviews(tail, (head :: currentGroup) :: rest)
        case _ =>
          groupConsecutiveReviews(tail, List(head) :: acc)
      }
  }
  
  // ============== CLOSURES IN SPARK TRANSFORMATIONS ==============
  
  /**
   * Creates a closure that captures threshold values for filtering
   * Demonstrates how Spark serializes and uses closures
   */
  def createQualityFilter(minRating: Double, minHelpfulness: Double) = {
    // These values are captured in the closure
    val ratingThreshold = minRating
    val helpfulnessThreshold = minHelpfulness
    
    // This function closes over the threshold values
    (review: ProcessedReview) => 
      review.rating >= ratingThreshold && 
      review.helpfulnessRatio >= helpfulnessThreshold
  }
  
  // ============== FUNCTIONAL ERROR HANDLING ==============
  
  /**
   * Safely parse JSON to Review with Try monad
   * Demonstrates functional error handling
   */
  def parseReviewSafely(json: String): Try[rawReview] = Try {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    implicit val formats = DefaultFormats
    
    parse(json).extract[rawReview]
  }
  
  /**
   * Result type for operations that can fail
   * Demonstrates custom error handling with Either
   */
  sealed trait ProcessingError
  case class InvalidData(message: String) extends ProcessingError
  case class ComputationError(message: String) extends ProcessingError
  
  def validateAndProcess(review: rawReview): Either[ProcessingError, ProcessedReview] = {
    if (review.reviewText.isEmpty) {
      Left(InvalidData("Review text is empty"))
    } else if (review.overall < 1 || review.overall > 5) {
      Left(InvalidData("Invalid rating value"))
    } else {
      Right(ProcessedReview(
        reviewerID = review.reviewerID,
        productID = review.asin,
        rating = review.overall,
        helpfulnessRatio = calculateHelpfulness(review.helpful),
        reviewLength = review.reviewText.length,
        sentiment = determineSentiment(review.overall),
        year = extractYear(review.unixReviewTime)
      ))
    }
  }
  
  // ============== SPARK TRANSFORMATIONS ==============
  
  /**
   * Main processing pipeline using DataFrame API
   * Demonstrates multiple Spark operations and function composition
   */
  def processReviews(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
    import spark.implicits._
    
    // Load data from external source
    val rawReviews: Dataset[rawReview] = spark.read
      .json(inputPath)
      .as[rawReview]
    
    // Transform using pure functions and closures
    val processedReviews: Dataset[ProcessedReview] = rawReviews
      .map(review => validateAndProcess(review))  // Using functional error handling
      .filter(_.isRight)  // Filter successful results
      .map(_.getOrElse(throw new RuntimeException("Unexpected error")))
      .filter(createQualityFilter(2.0, 0.3))  // Using closure
    
    // Cache for multiple operations
    processedReviews.cache()
    
    // Operation 1: GroupBy and aggregate for product statistics
    val productStats = processedReviews
      .groupBy("productID")
      .agg(
        avg("rating").as("avgRating"),
        count("*").as("reviewCount"),
        avg("helpfulnessRatio").as("avgHelpfulness"),
        mode("sentiment").as("dominantSentiment")
      )
      .as[ProductStats]
    
    // Operation 2: Join operation to enrich data
    val enrichedReviews = processedReviews
      .join(productStats, processedReviews("productID") === productStats("productID"))
      .select(
        processedReviews("*"),
        productStats("avgRating").as("productAvgRating"),
        productStats("reviewCount").as("productReviewCount")
      )
    
    // Operation 3: Window functions for ranking
    import org.apache.spark.sql.expressions.Window
    val windowSpec = Window.partitionBy("year").orderBy(desc("helpfulnessRatio"))
    
    val rankedReviews = enrichedReviews
      .withColumn("helpfulnessRank", rank().over(windowSpec))
      .filter(col("helpfulnessRank") <= 100)  // Top 100 most helpful per year
    
    // Operation 4: RDD operations for custom aggregations
    val reviewerProfiles: RDD[ReviewerProfile] = processedReviews.rdd
      .map(r => (r.reviewerID, r))
      .groupByKey()
      .mapValues(reviews => {
        val reviewList = reviews.toList
        val count = reviewList.length
        ReviewerProfile(
          reviewerID = reviewList.head.reviewerID,
          totalReviews = count,
          avgRating = reviewList.map(_.rating).sum / count,
          avgReviewLength = reviewList.map(_.reviewLength.toDouble).sum / count,
          helpfulnessScore = reviewList.map(_.helpfulnessRatio).sum / count
        )
      })
      .values
    
    // Save results to files
    productStats.write.mode("overwrite").json(s"$outputPath/product_stats")
    rankedReviews.write.mode("overwrite").parquet(s"$outputPath/ranked_reviews")
    reviewerProfiles.toDF().write.mode("overwrite").json(s"$outputPath/reviewer_profiles")
    
    // Demonstrate function composition for final summary
    val summaryPipeline = pipe(
      filterByMinRating(4.0) _,
      whenThen((r: ProcessedReview) => r.reviewLength > 100)(identity)
    )
    
    val highQualityReviews = processedReviews
      .filter(summaryPipeline)
      .count()
    
    println(s"Total high-quality reviews: $highQualityReviews")
  }
  
  // ============== MAIN ENTRY POINT ==============
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Amazon Reviews Functional Analysis")
      .master("local[*]")
      .getOrCreate()
    
    try {
      val inputPath = args.headOption.getOrElse("data/reviews.json")
      val outputPath = args.lift(1).getOrElse("output/")
      
      processReviews(spark, inputPath, outputPath)
      
    } finally {
      spark.stop()
    }
  }
}