package hydra.sql

import java.sql.{BatchUpdateException, Connection}

import hydra.avro.convert.IsoDate
import hydra.avro.io.SaveMode.SaveMode
import hydra.avro.io._
import hydra.avro.util.{AvroUtils, SchemaWrapper}
import hydra.common.util.TryWith
import org.apache.avro.LogicalTypes.LogicalTypeFactory
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{LogicalType, LogicalTypes, Schema}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.control.NonFatal
import scala.util.{Failure, Try}


/**
  * Created by alexsilva on 7/11/17.
  *
  * A batch size of 0 means that this class will never do any executeBatch and that external clients need to call
  * flush()
  *
  * If the primary keys are provided as a constructor argument, it overrides anything that
  * may have been provided by the schema.
  *
  * Delete operations happen immediately, even when submitted through the batch operation. This behavior may
  * affect the performance of systems that process many deletes.
  *
  * @param settings        The JdbcWriterSettings to be used
  * @param schemaWrapper          The initial schema to use when creating/updating/inserting records.
  * @param mode            See [hydra.avro.io.SaveMode]
  * @param tableIdentifier The table identifier; defaults to using the schema's name if none provided.
  */
class JdbcRecordWriter(val settings: JdbcWriterSettings,
                       val connectionProvider: ConnectionProvider,
                       val schemaWrapper: SchemaWrapper,
                       val mode: SaveMode = SaveMode.ErrorIfExists,
                       tableIdentifier: Option[TableIdentifier] = None) extends RecordWriter with JdbcHelper {

  import JdbcRecordWriter._

  logger.debug("Initializing JdbcRecordWriter")

  private val batchSize = settings.batchSize

  private val syntax = settings.dbSyntax

  private val dialect = JdbcDialects.get(connectionProvider.connectionUrl)

  private val store: Catalog = new JdbcCatalog(connectionProvider, syntax, dialect)

  private val tableId = tableIdentifier.getOrElse(TableIdentifier(JdbcUtils.createTableNameFromSchema(schemaWrapper.schema)))

  private val operations = new mutable.ArrayBuffer[Operation]()

  private var currentSchema = schemaWrapper

  private val tableObj: Table = {
    val tableExists = store.tableExists(tableId)
    mode match {
      case SaveMode.ErrorIfExists if tableExists =>
        throw new AnalysisException(s"Table ${tableId.table} already exists.")
      case SaveMode.Overwrite => //todo: truncate table
        Table(tableId.table, schemaWrapper, tableId.database)
      case _ =>
        val table = Table(tableId.table, schemaWrapper, tableId.database)
        store.createOrAlterTable(table)
        table
    }
  }

  private val name = syntax.format(tableObj.name)

  private var valueSetter = new AvroValueSetter(schemaWrapper, dialect)

  private var upsertStmt = dialect.upsert(syntax.format(name), schemaWrapper, syntax)

  //since changing pks on a table isn't supported, this can be a val
  private val deleteStmt =
    schemaWrapper.primaryKeys.headOption.map(_ =>
      dialect.deleteStatement(syntax.format(name), schemaWrapper.primaryKeys, syntax))

  private def connection = connectionProvider.getConnection

  override def batch(operation: Operation): Unit = {
    operation match {
      case u@Upsert(_) => add(u)
      case DeleteByKey(keys) =>
        flush()
        delete(keys)
    }
  }

  private def maybeFlush() = if (batchSize > 0 && operations.size >= batchSize) flush()

  private def add(op: Upsert): Unit = {
    if (AvroUtils.areEqual(currentSchema.schema, op.record.getSchema)) {
      operations += op
      maybeFlush()
    }
    else {
      // Each batch needs to have the same dbInfo, so get the buffered records out, reset state if possible,
      // add columns and re-attempt the add
      flush()
      updateDb(op.record)
      add(op)
    }
  }

  private def updateDb(record: GenericRecord): Unit = synchronized {
    val cpks = currentSchema.primaryKeys
    val wrapper = SchemaWrapper.from(record.getSchema, cpks)
    store.createOrAlterTable(Table(tableId.table, wrapper))
    currentSchema = wrapper
    upsertStmt = dialect.upsert(syntax.format(name), currentSchema, syntax)
    valueSetter = new AvroValueSetter(currentSchema, dialect)
  }

  /**
    * Convenience method to write exactly one record to the underlying database.
    *
    * @param record
    */
  private def upsert(record: GenericRecord): Try[Unit] = {
    if (AvroUtils.areEqual(currentSchema.schema, record.getSchema)) {
      TryWith(connection.prepareStatement(upsertStmt)) { pstmt =>
        valueSetter.bind(record, pstmt)
        pstmt.executeUpdate()
      } //TODO: better error handling here, we do the get just so that we throw an exception if there is one.
    }
    else {
      updateDb(record)
      upsert(record)
    }
  }

  private def deleteError() =
    throw new UnsupportedOperationException("Deletes are not possible without a primary key.")

  /**
    * Convenience method to delete exactly one record from the underlying database.
    *
    * @param keys
    */
  private def delete(keys: Map[String, AnyRef]): Try[Unit] = {
    deleteStmt match {
      case Some(s) =>
        TryWith(connection.prepareStatement(s)) { dstmt =>
          val fields = keys.map(v => schemaWrapper.schema.getField(v._1) -> v._2)
          valueSetter.bind(schemaWrapper.schema, fields, dstmt)
          dstmt.executeUpdate()
        } //TODO: better error handling here, we do the get just so that we throw an exception if there is one.

      case None => deleteError()
    }
  }

  override def execute(operation: Operation): Unit = {
    operation match {
      case Upsert(record) => upsert(record)
      case DeleteByKey(fields) => delete(fields)
    }
  }

  def supportsTransactions(conn: Connection): Boolean = {
    try {
      conn.getMetaData().supportsDataManipulationTransactionsOnly() ||
        conn.getMetaData().supportsDataDefinitionAndDataManipulationTransactions()

    } catch {
      case NonFatal(e) =>
        JdbcRecordWriter.logger.warn("Exception while detecting transaction support", e)
        true
    }
  }

  def flush(): Unit = synchronized {
    val conn = connectionProvider.getConnection

    var committed = false

    val supportsTxn = supportsTransactions(conn)

    if (supportsTxn) conn.setAutoCommit(false) // Everything in the same db transaction.

    val upsert = conn.prepareStatement(upsertStmt)
    lazy val delete = conn.prepareStatement(deleteStmt.get)

    operations.foreach {
      case Upsert(record) => valueSetter.bind(record, upsert)
      case DeleteByKey(keys) =>
        val fields = keys.map(v => schemaWrapper.schema.getField(v._1) -> v._2)
        valueSetter.bind(schemaWrapper.schema, fields, delete)
    }
    try {
      upsert.executeBatch()
      if (supportsTxn) conn.commit()
      committed = true
    }
    catch {
      case e: BatchUpdateException =>
        logger.error("Batch update error", e.getNextException())
        conn.rollback()
        val recordsInError = handleBatchError(operations)
        conn.commit()
        logger.error(s"The following records could not be replicated to table $name:")
        recordsInError.foreach(r => logger.error(s"${r._1.toString} - [${r._2.getMessage}]"))
      case e: Exception =>
        throw e
    }
    finally {
      if (!committed && supportsTxn) conn.rollback()

      conn.setAutoCommit(true) //back
    }
    operations.clear()
  }

  def close(): Unit = {
    flush()
  }

  /**
    * Try running the batch statements, one record at a time
    *
    * Returns the generic record(s) that caused the failure.
    */
  private[sql] def handleBatchError(records: Seq[Operation]): Seq[(Operation, Throwable)] = {
    operations.map { operation =>
      val result: Try[Unit] = operation match {
        case Upsert(record) => upsert(record)
        case DeleteByKey(keys) => delete(keys)
      }
      operation -> result
    }.filter(_._2.isFailure).map(x => x._1 -> Failure(x._2.failed.get).exception)
  }
}

object JdbcRecordWriter {

  LogicalTypes.register(IsoDate.IsoDateLogicalTypeName, new LogicalTypeFactory {
    override def fromSchema(schema: Schema): LogicalType = IsoDate
  })

  val logger = LoggerFactory.getLogger(getClass)
}
