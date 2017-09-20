
import org.apache.spark.sql.hive.HiveContext

trait TableGenerator {
	def generate(hiveCtx: HiveContext, opts: Map[String, String])
}