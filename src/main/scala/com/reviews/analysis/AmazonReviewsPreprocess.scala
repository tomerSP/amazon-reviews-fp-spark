package com.reviews.analysis

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Handles all data I/O operations for the Amazon reviews project.
 * This class is responsible for loading the data from an external source
 * and providing the initial Spark Dataset for processing.
 *
 * @param spark The active SparkSession.
 */
class AmazonReviewsPreprocess(spark: SparkSession) {

    import spark.implicits._

    /**
     * Loads the raw review data from a JSON file into a Spark Dataset.
     * Uses a functional approach to handle file loading errors by returning an Option.
     *
     * @param inputPath The path to the JSON data file.
     * @return An Option[Dataset[Review]] which will be Some(Dataset) on success,
     * or None on failure (e.g., file not found).
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

/** Represents a raw Amazon review from JSON */
case class Review(
                   reviewerID: String,
                   asin: String,           // Amazon Standard Identification Number
                   reviewerName: Option[String],
                   vote: Option[String],   // number of "helpful" votes as a string
                   reviewText: Option[String],
                   overall: Double,        // Rating (1-5)
                   summary: Option[String],
                   unixReviewTime: Long,
                   reviewTime: String,
                   image: Option[List[String]],
                   verified: Option[Boolean]
                 )