package com.reviews.analysis

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.time.{Instant, ZoneId}
import scala.util.Try

/**
 * Encapsulates the core functional logic for analyzing Amazon reviews.
 * This object contains only pure functions and Spark transformations,
 * with no direct I/O operations.
 */
object AmazonReviewsAnalysis {

    // ============== PURE FUNCTIONS (Functional Core) ==============

    /** Extracts helpful votes from an optional string and converts to an Int. */
    private[reviews] val getHelpfulnessVotes: Option[String] => Int = {
        case Some(votes) => Try(votes.toInt).getOrElse(0)
        case None => 0
    }

    /** Determines sentiment based on rating. */
    private[reviews] val determineSentiment: Double => String = {
        case rating if rating >= 4.0 => "positive"
        case rating if rating >= 3.0 => "neutral"
        case _ => "negative"
    }

    /** Extracts year from a Unix timestamp. */
    private[reviews] val extractYear: Long => Int = timestamp => {
        val instant = Instant.ofEpochSecond(timestamp)
        val zonedDateTime = instant.atZone(ZoneId.systemDefault())
        zonedDateTime.getYear
    }

    // ============== CUSTOM COMBINATOR ==============

    /** Encapsulates combinator-based review filters */
    object ReviewFilters {

        // Type alias for clarity
        private type ReviewPredicate = ProcessedReview => Boolean

        /** Combinator to combine predicates with logical AND */
        def and(pred1: ReviewPredicate, pred2: ReviewPredicate): ReviewPredicate =
            review => pred1(review) && pred2(review)

        // ============== CURRIED FUNCTIONS ==============

        /** Custom combinator: applies a transformation only if the predicate is true */
        def whenThen[A](predicate: A => Boolean)(transform: A => A): A => A =
            (input: A) => if (predicate(input)) transform(input) else input

        /** Predicate: minimum rating */
        def filterByMinRating(minRating: Double): ReviewPredicate =
            review => review.rating >= minRating

        /** Predicate: minimum review length */
        def filterByMinLength(minLength: Int): ReviewPredicate =
            review => review.reviewLength >= minLength
    }


    // ============== CLOSURES IN SPARK TRANSFORMATIONS ==============

    /** Creates a closure that captures threshold values for filtering. */
    private[reviews] def createQualityFilter(minRating: Double, minHelpfulness: Int): ProcessedReview => Boolean = {
        val ratingThreshold = minRating
        val helpfulnessThreshold = minHelpfulness
        (review: ProcessedReview) =>
            review.rating >= ratingThreshold &&
              review.helpfulnessVotes >= helpfulnessThreshold
    }

    // ============== FUNCTIONAL ERROR HANDLING ==============

    trait ProcessingError
    case class InvalidData(message: String) extends ProcessingError

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

    def processReviews(spark: SparkSession, rawReviews: Dataset[Review]): Dataset[ProcessedReview] = {
        import spark.implicits._
        val processedReviews: Dataset[ProcessedReview] = rawReviews
          .map(review => validateAndProcess(review))
          .filter(_.isRight)
          .map(_.right.get)
          .filter(createQualityFilter(2.0, 10))
        processedReviews
    }

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

    def enrichReviews(processedReviews: Dataset[ProcessedReview], productStats: Dataset[ProductStats]): DataFrame = {
        processedReviews
          .join(productStats, processedReviews("productID") === productStats("productID"))
          .select(
              processedReviews("*"),
              productStats("avgRating").as("productAvgRating"),
              productStats("reviewCount").as("productReviewCount")
          )
    }

    def rankReviews(enrichedReviews: DataFrame): DataFrame = {
        import org.apache.spark.sql.expressions.Window
        val windowSpec = Window.partitionBy("year").orderBy(desc("helpfulnessVotes"))
        enrichedReviews
          .withColumn("helpfulnessRank", rank().over(windowSpec))
          .filter(col("helpfulnessRank") <= 100)
    }

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

/** Processed review with additional computed fields */
case class ProcessedReview(
                            reviewerID: String,
                            productID: String,
                            rating: Double,
                            helpfulnessVotes: Int,  // Changed to an Int since ratio is not available
                            reviewLength: Int,
                            sentiment: String,
                            year: Int,
                            isHighQuality: Boolean = false
                          )

/** Product statistics aggregate */
case class ProductStats(
                         productID: String,
                         avgRating: Double,
                         reviewCount: Long,
                         avgHelpfulness: Double,
                       )

/** Reviewer profile aggregate */
case class ReviewerProfile(
                            reviewerID: String,
                            totalReviews: Long,
                            avgRating: Double,
                            avgReviewLength: Double,
                            helpfulnessScore: Double
                          )