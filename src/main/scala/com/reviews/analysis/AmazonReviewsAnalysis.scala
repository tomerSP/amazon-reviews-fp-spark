package com.reviews.analysis

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.time.{Instant, ZoneId}
import scala.util.Try

/**
 * Encapsulates the core functional logic for analyzing Amazon reviews.
 *
 * This object contains pure functions and Spark transformations, with no direct I/O operations.
 * It serves as the "Functional Core" of the application as required by the project
 * specifications.
 */
object AmazonReviewsAnalysis {

    // ============== PURE FUNCTIONS (Functional Core) ==============

    /**
     * Extracts helpful votes from an optional string and converts it to an integer.
     * This is a pure function as it has no side effects and its output depends only on its input.
     *
     * @param votes The helpfulness votes as an Option[String].
     * @return The number of helpful votes as an Int, defaulting to 0 if the input is None or invalid.
     */
    private[reviews] val getHelpfulnessVotes: Option[String] => Int = {
        case Some(votes) => Try(votes.toInt).getOrElse(0)
        case None => 0
    }

    /**
     * Determines the sentiment of a review based on its rating.
     * This is a pure function that maps a rating value to a sentiment string.
     *
     * @param rating The review's overall rating (Double).
     * @return A sentiment string ("positive", "neutral", or "negative").
     */
    private[reviews] val determineSentiment: Double => String = {
        case rating if rating >= 4.0 => "positive"
        case rating if rating >= 3.0 => "neutral"
        case _ => "negative"
    }

    /**
     * Extracts the year from a Unix timestamp.
     * This is a pure function that uses `java.time` for a deterministic transformation.
     *
     * @param timestamp The Unix timestamp in seconds.
     * @return The year as an Int.
     */
    private[reviews] val extractYear: Long => Int = timestamp => {
        val instant = Instant.ofEpochSecond(timestamp)
        val zonedDateTime = instant.atZone(ZoneId.systemDefault())
        zonedDateTime.getYear
    }

    // ============== CUSTOM COMBINATOR ==============

    /**
     * Encapsulates combinator-based review filters.
     *
     * This object demonstrates the use of function combinators to build more complex
     * functions from simpler ones, fulfilling a key project requirement.
     */
    object ReviewFilters {

        // Type alias for clarity
        private type ReviewPredicate = ProcessedReview => Boolean

        /**
         * A custom combinator that combines two predicates with a logical AND.
         *
         * @param pred1 The first predicate function.
         * @param pred2 The second predicate function.
         * @return A new predicate that returns true only if both input predicates are true.
         */
        def and(pred1: ReviewPredicate, pred2: ReviewPredicate): ReviewPredicate =
            review => pred1(review) && pred2(review)

        // ============== CURRIED FUNCTIONS ==============

        /**
         * A custom combinator that applies a transformation only if a given predicate is true.
         * This function is curried, allowing for partial application of the predicate.
         *
         * @param predicate The condition to check.
         * @return A function that takes a transformation and returns a new function.
         */
        def whenThen[A](predicate: A => Boolean)(transform: A => A): A => A =
            (input: A) => if (predicate(input)) transform(input) else input

        /**
         * A curried function that creates a predicate to filter reviews by a minimum rating.
         *
         * @param minRating The minimum rating threshold.
         * @return A predicate function that checks if a review's rating meets the threshold.
         */
        def filterByMinRating(minRating: Double): ReviewPredicate =
            review => review.rating >= minRating

        /**
         * A curried function that creates a predicate to filter reviews by minimum length.
         *
         * @param minLength The minimum review text length threshold.
         * @return A predicate function that checks if a review's length meets the threshold.
         */
        def filterByMinLength(minLength: Int): ReviewPredicate =
            review => review.reviewLength >= minLength
    }


    // ============== CLOSURES IN SPARK TRANSFORMATIONS ==============

    /**
     * Creates a closure that captures threshold values for filtering reviews.
     * This demonstrates the use of closures, where the returned function
     * retains access to the `minRating` and `minHelpfulness` variables.
     *
     * @param minRating The minimum rating threshold to capture.
     * @param minHelpfulness The minimum helpfulness votes threshold to capture.
     * @return A function that filters a `ProcessedReview` based on the captured thresholds.
     */
    private[reviews] def createQualityFilter(minRating: Double, minHelpfulness: Int): ProcessedReview => Boolean = {
        val ratingThreshold = minRating
        val helpfulnessThreshold = minHelpfulness
        (review: ProcessedReview) =>
            review.rating >= ratingThreshold &&
              review.helpfulnessVotes >= helpfulnessThreshold
    }

    // ============== FUNCTIONAL ERROR HANDLING ==============

    /** Represents a sealed trait for processing errors, enabling pattern matching. */
    trait ProcessingError
    /** A case class representing an invalid data error. */
    case class InvalidData(message: String) extends ProcessingError

    /**
     * Validates and processes a raw `Review` object.
     * This function uses `Either` for functional error handling, returning `Right` on success
     * and `Left` with a `ProcessingError` on failure.
     *
     * @param review The raw `Review` to validate.
     * @return An `Either[ProcessingError, ProcessedReview]`.
     */
    private[reviews] def validateAndProcess(review: Review): Either[ProcessingError, ProcessedReview] = {
        if (review.reviewText.isEmpty) {
            Left(InvalidData("Review text is empty or missing"))
        } else if (review.overall < 1 || review.overall > 5) {
            Left(InvalidData("Invalid rating value"))
        } else {
            Right(ProcessedReview(
                reviewerID = review.reviewerID,
                productID = review.asin,
                rating = review.overall,
                helpfulnessVotes = getHelpfulnessVotes(review.vote),
                reviewLength = review.reviewText.getOrElse("").length,
                sentiment = determineSentiment(review.overall),
                year = extractYear(review.unixReviewTime)
            ))
        }
    }

    // ============== SPARK TRANSFORMATIONS ==============

    /**
     * Processes a dataset of raw reviews, applying a series of functional transformations.
     * This function uses `map` for transformation and `filter` to remove invalid records.
     *
     * @param spark The SparkSession.
     * @param rawReviews The input Dataset of raw `Review` objects.
     * @return A Dataset of validated and processed `ProcessedReview` objects.
     */
    def processReviews(spark: SparkSession, rawReviews: Dataset[Review]): Dataset[ProcessedReview] = {
        import spark.implicits._
        val processedReviews: Dataset[ProcessedReview] = rawReviews
          .map(review => validateAndProcess(review))
          .filter(_.isRight)
          .map(_.right.get)
          .filter(createQualityFilter(2.0, 10))
        processedReviews
    }

    /**
     * Generates aggregate statistics for each product.
     * This transformation uses Spark's `groupBy` and `agg` operations.
     *
     * @param spark The SparkSession.
     * @param processedReviews The Dataset of `ProcessedReview` objects.
     * @return A Dataset of `ProductStats`.
     */
    def generateProductStats(spark: SparkSession, processedReviews: Dataset[ProcessedReview]): Dataset[ProductStats] = {
        import spark.implicits._
        processedReviews
          .groupBy("productID")
          .agg(
              avg("rating").as("avgRating"),
              count("*").as("reviewCount"),
              avg("helpfulnessVotes").as("avgHelpfulness"),
          )
          .as[ProductStats]
    }

    /**
     * Enriches the processed reviews with product-level statistics.
     * This function performs a `join` operation between two datasets.
     *
     * @param processedReviews The Dataset of `ProcessedReview` objects.
     * @param productStats The Dataset of `ProductStats`.
     * @return A DataFrame with enriched review data.
     */
    def enrichReviews(processedReviews: Dataset[ProcessedReview], productStats: Dataset[ProductStats]): DataFrame = {
        processedReviews
          .join(productStats, processedReviews("productID") === productStats("productID"))
          .select(
              processedReviews("*"),
              productStats("avgRating").as("productAvgRating"),
              productStats("reviewCount").as("productReviewCount")
          )
    }

    /**
     * Ranks reviews by helpfulness within each year and filters for the top 100.
     * This transformation uses a Spark Window function (`rank`) and a `filter`.
     *
     * @param enrichedReviews The DataFrame of enriched reviews.
     * @return A DataFrame containing the top 100 most helpful reviews per year.
     */
    def rankReviews(enrichedReviews: DataFrame): DataFrame = {
        import org.apache.spark.sql.expressions.Window
        val windowSpec = Window.partitionBy("year").orderBy(desc("helpfulnessVotes"))
        enrichedReviews
          .withColumn("helpfulnessRank", rank().over(windowSpec))
          .filter(col("helpfulnessRank") <= 100)
    }

    /**
     * Generates profiles for each reviewer using the RDD API.
     * This demonstrates `map` and `groupByKey` transformations on an RDD.
     *
     * @param processedReviews The Dataset of `ProcessedReview` objects.
     * @return An RDD of `ReviewerProfile` objects.
     */
    def generateReviewerProfiles(processedReviews: Dataset[ProcessedReview]): RDD[ReviewerProfile] = {
        processedReviews.rdd
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
                  helpfulnessScore = reviewList.map(_.helpfulnessVotes.toDouble).sum / count
              )
          })
          .values
    }
}

// ============== CASE CLASSES (Immutable Data Structures) ==============

/**
 * Represents a processed review with additional computed fields.
 * Case classes are used as immutable data structures.
 *
 * @param reviewerID The unique ID of the reviewer.
 * @param productID The ID of the product.
 * @param rating The numerical rating (1-5).
 * @param helpfulnessVotes The number of helpful votes.
 * @param reviewLength The length of the review text.
 * @param sentiment The computed sentiment ("positive", "neutral", "negative").
 * @param year The year the review was posted.
 * @param isHighQuality A flag to mark a review as high quality.
 */
case class ProcessedReview(
                            reviewerID: String,
                            productID: String,
                            rating: Double,
                            helpfulnessVotes: Int,
                            reviewLength: Int,
                            sentiment: String,
                            year: Int,
                            isHighQuality: Boolean = false
                          )

/**
 * Represents aggregate statistics for a product.
 *
 * @param productID The ID of the product.
 * @param avgRating The average rating for the product.
 * @param reviewCount The total number of reviews for the product.
 * @param avgHelpfulness The average helpfulness score for the product's reviews.
 */
case class ProductStats(
                         productID: String,
                         avgRating: Double,
                         reviewCount: Long,
                         avgHelpfulness: Double,
                       )

/**
 * Represents an aggregate profile for a reviewer.
 *
 * @param reviewerID The unique ID of the reviewer.
 * @param totalReviews The total number of reviews written by the reviewer.
 * @param avgRating The average rating given by the reviewer.
 * @param avgReviewLength The average length of the reviewer's reviews.
 * @param helpfulnessScore The average helpfulness score of the reviewer's reviews.
 */
case class ReviewerProfile(
                            reviewerID: String,
                            totalReviews: Long,
                            avgRating: Double,
                            avgReviewLength: Double,
                            helpfulnessScore: Double
                          )