package com.reviews.analysis

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Handles all data I/O operations for the Amazon reviews project.
 *
 * This class is responsible for loading the data from an external source and
 * providing the initial Spark Dataset for processing. It clearly separates
 * I/O from the pure logic, as required by the project.
 *
 * @param spark The active SparkSession.
 */
class AmazonReviewsPreprocess(spark: SparkSession) {

    import spark.implicits._

    /**
     * Loads the raw review data from a JSON file into a Spark Dataset.
     * Uses a functional approach to handle file loading errors by returning an `Option`.
     *
     * @param inputPath The path to the JSON data file.
     * @return An `Option[Dataset[Review]]` which will be `Some(Dataset)` on success,
     * or `None` on failure (e.g., file not found).
     */
    def loadReviews(inputPath: String): Option[Dataset[Review]] = {
        try {
            Some(
                spark.read
                  .json(inputPath)
                  .as[Review]
            )
        }
        catch {
            case _: Exception => None
        }
    }
}

// ============== CASE CLASS FOR RAW DATA (Immutable Data Structure) ==============

/**
 * Represents a raw Amazon review from a JSON data source.
 * This immutable case class is used to model the input data.
 *
 * @param reviewerID The ID of the reviewer.
 * @param asin The Amazon Standard Identification Number (product ID).
 * @param reviewerName The name of the reviewer.
 * @param vote The number of "helpful" votes as a string.
 * @param reviewText The text of the review.
 * @param overall The rating (1-5).
 * @param summary The summary of the review.
 * @param unixReviewTime The review time as a Unix timestamp.
 * @param reviewTime The review time as a formatted string.
 * @param image An optional list of image URLs.
 * @param verified An optional boolean indicating if the review is verified.
 */
case class Review(
                   reviewerID: String,
                   asin: String,
                   reviewerName: Option[String],
                   vote: Option[String],
                   reviewText: Option[String],
                   overall: Double,
                   summary: Option[String],
                   unixReviewTime: Long,
                   reviewTime: String,
                   image: Option[List[String]],
                   verified: Option[Boolean]
                 )