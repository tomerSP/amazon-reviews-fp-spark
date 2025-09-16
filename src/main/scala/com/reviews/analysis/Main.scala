package com.reviews.analysis

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Main application entry point.
 * This object orchestrates the data pipeline by separating I/O
 * from the core functional logic.
 */
object Main {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
          .appName("Amazon Reviews Functional Analysis")
          .master("local[*]")
          .getOrCreate()

        val inputPath: String = "src/main/scala/com/reviews/data/reviews.json"
        val outputPath: String = "src/main/scala/com/reviews/data/"

        import spark.implicits._

        val preprocessor = new AmazonReviewsPreprocess(spark)

        // I/O: Load data using the preprocessor
        // ============== PATTERN MATCHING WITH CASE CLASSES ==============
        val rawReviews: Dataset[Review] = preprocessor.loadReviews(inputPath) match {
            case Some(ds: Dataset[Review]) => ds
            case None =>
                val emptyReview = Review(
                    reviewerID = "",
                    asin = "",
                    reviewerName = None,
                    vote = None,
                    reviewText = None,
                    overall = 0.0,
                    summary = None,
                    unixReviewTime = 0L,
                    reviewTime = "",
                    image = None,
                    verified = None
                )
                spark.createDataset(Seq(emptyReview))
        }

        // Pure Logic: Process the data using the analysis object
        val processedReviews = AmazonReviewsAnalysis.processReviews(spark, rawReviews)
        processedReviews.cache()

        // Pure Logic: Generate and collect results
        val productStats = AmazonReviewsAnalysis.generateProductStats(spark, processedReviews)
        val enrichedReviews = AmazonReviewsAnalysis.enrichReviews(processedReviews, productStats)
        val rankedReviews = AmazonReviewsAnalysis.rankReviews(enrichedReviews)
        val reviewerProfiles = AmazonReviewsAnalysis.generateReviewerProfiles(processedReviews)

        // I/O: Save results to files
        productStats.write.mode("overwrite").json(s"$outputPath/product_stats")
        rankedReviews.write.mode("overwrite").parquet(s"$outputPath/ranked_reviews")
        reviewerProfiles.toDF().write.mode("overwrite").json(s"$outputPath/reviewer_profiles")

        // Demonstrate function composition with custom combinators for final summary
        import com.reviews.analysis.AmazonReviewsAnalysis.ReviewFilters._

        val markAsHighQuality: ProcessedReview => ProcessedReview =
            review => review.copy(isHighQuality = true)

        //Partial function application
        val summaryPredicate = and(filterByMinRating(4.0), filterByMinLength(100))

        val summaryPipeline = whenThen(summaryPredicate)(markAsHighQuality)

        val highQualityReviews = processedReviews
          .map(summaryPipeline)
          .filter(_.isHighQuality)
          .count()

        println(s"Total high-quality reviews: $highQualityReviews")

        spark.stop()
    }
}