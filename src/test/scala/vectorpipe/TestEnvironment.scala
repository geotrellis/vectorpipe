/*
 * Copyright 2016 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package vectorpipe

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.scalatest._

object TestEnvironment {
}

/*
 * These set of traits handle the creation and deletion of test directories on the local fs and hdfs,
 * It uses commons-io in at least one case (recursive directory deletion)
 */
trait TestEnvironment extends BeforeAndAfterAll { self: Suite with BeforeAndAfterAll =>
  implicit val ss: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("VectorPipe Test")
    .config("spark.ui.enabled", "false")
    .config("spark.default.parallelism","8")
    .config("spark.serializer", classOf[KryoSerializer].getName)
    .config("spark.kryo.registrationRequired", "false")
    .config("spark.kryoserializer.buffer.max", "500m")
    .config("spark.sql.orc.impl", "native")
    .getOrCreate()

  // get the name of the class which mixes in this trait
  val name = this.getClass.getName

  override def beforeAll() = {
    ss.sparkContext.setJobGroup(this.getClass.getName, "test")
  }

  override def afterAll() = {
    ss.sparkContext.clearJobGroup()
  }
}
