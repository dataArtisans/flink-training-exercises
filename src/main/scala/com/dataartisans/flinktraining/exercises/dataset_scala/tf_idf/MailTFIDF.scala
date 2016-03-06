/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flinktraining.exercises.dataset_scala.tf_idf

import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool

import com.dataartisans.flinktraining.dataset_preparation.MBoxParser

/**
 * Scala reference implementation for the "TF-IDF" exercise of the Flink training.
 * The task of the exercise is to compute the TF-IDF score for words in mails of the
 * Apache Flink developer mailing list archive.
 *
 * Required parameters:
 *   --input path-to-input-directory
 *
 */
object MailTFIDF {

  // stop words
  private val STOP_WORDS = Set (
    "the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is", "there", "it", "this",
    "that", "on", "was", "by", "of", "to", "in", "to", "message", "not", "be", "with", "you",
    "have", "as", "can")

  // word pattern regular expression
  private val WORD_PATTERN = "(\\p{Alpha})+".r

  def main(args: Array[String]) {

    // parse parameters
    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // function returns true if string is not a stop word and matches the word pattern regex
    val isWord : String => Boolean = s => !STOP_WORDS.contains(s) && WORD_PATTERN.unapplySeq(s).isDefined

    // read messageId and body field of the input data
    val mails : DataSet[(String,Array[String])] = env.readCsvFile[(String, String)](
      input,
      lineDelimiter = MBoxParser.MAIL_RECORD_DELIM,
      fieldDelimiter = MBoxParser.MAIL_FIELD_DELIM,
      includedFields = Array(0,4)
    ).map (m => (m._1, m._2.toLowerCase.split("\\s")  // convert message to lower case and tokenize (split on space)
     .filter(s => isWord(s)))) // retain only those strings that are words

    // count mails in data set
    val cnt = mails.count.toDouble

    // For each mail, compute the frequency of words in a document
    val tfs : DataSet[(String, String, Int)] =
      mails flatMap (m => m._2.groupBy(w => w)  // For each word in a document, create a Map where
                                                // the key is the word and the value is an Array containing all
                                                // occurrences of that word.
                              .map(e => (m._1, e._1, e._2.length))) // for each entry in the Map, create a tuple consisting
                                                                    // m._1 is the document id
                                                                    // e._1 is the word in the document
                                                                    // e._2 is the Array of occurrences. Thus, e._2.length
                                                                    // is the number of occurrences of the word in the document

    // compute document frequency (number of mails that contain a word at least once)
    val dfs : DataSet[(String, Int)] =
      (mails flatMap (m => m._2.toSet)) // Extract unique words of each mail converting Array[String] to a Set[String]
        .map (m => (m, 1))  // Create the (word, 1) initial record for each unique string
        .groupBy(0)         // group by the word
        .sum(1)             // sum the 1's; computes the count the number of mails for each word

    // compute TF-IDF score from TF, DF, and total number of mails
    val tfidfs : DataSet[(String, String, Double)] =
      tfs.join(dfs).where(1).equalTo(0) { (l, r) => (l._1, l._2, l._3 * (cnt / r._2) ) }

    tfidfs.print
  }
}

