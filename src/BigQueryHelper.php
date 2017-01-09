<?php
namespace Packaged\BigQuery;

use Packaged\Helpers\Objects;

class BigQueryHelper
{
  const ERR_OK = 0; // no error
  const ERR_RETRY = 1; // error that needs a retry (normal exponential backoff)
  const ERR_DELAYED_RETRY = 2; // error that needs a delayed retry (e.g. after creating a table)
  const ERR_FATAL = 3; // fatal error that can't be retried
  const ERR_UNKNOWN = 4; // Any other error

  private $_gcpProject;
  /** @var \Google_Client */
  private $_client = null;
  private $_service = null;
  private $_dataSet = null;
  private $_tableRefs = [];
  private $_tableSchemas = [];
  private $_debugEnabled = false;
  // this is used to prevent us attempting to create the dataset multiple times
  private $_dataSetCreated = false;
  // Used to stop multiple create table calls for the same table
  private $_createdTables = [];

  public function __construct(
    \Google_Client $client, $gcpProject, $dataSet = null, $debugEnabled = false
  )
  {
    $this->_client = $client;
    $this->_gcpProject = $gcpProject;
    $this->_debugEnabled = $debugEnabled;
    if($dataSet)
    {
      $this->_dataSet = $dataSet;
    }
  }

  protected function _debug($msg)
  {
    if($this->_debugEnabled)
    {
      $this->_log($msg);
    }
  }

  protected function _log($msg)
  {
    error_log('BigQueryHelper: ' . $msg);
  }

  public function setDebugEnabled($debugEnabled)
  {
    $this->_debugEnabled = $debugEnabled;
    return $this;
  }

  public function isDebugEnabled()
  {
    return $this->_debugEnabled;
  }

  public function bigQueryProject()
  {
    return $this->_gcpProject;
  }

  /**
   * @return \Google_Client
   */
  public function getClient()
  {
    return $this->_client;
  }

  /**
   * @return \Google_Service_Bigquery
   */
  public function getService()
  {
    if($this->_service === null)
    {
      $this->_service = new \Google_Service_Bigquery($this->getClient());
    }
    return $this->_service;
  }

  public function setDataSet($dataSet)
  {
    $this->_dataSet = $dataSet;
    return $this;
  }

  public function getDataSet()
  {
    return $this->_dataSet;
  }

  /**
   * @param $bqTableName
   *
   * @return \Google_Service_Bigquery_TableReference
   * @throws \Exception
   */
  public function getTableReference($bqTableName)
  {
    $bqTableName = $this->_trimDatasetFromTableName($bqTableName);

    if(!isset($this->_tableRefs[$bqTableName]))
    {
      $tableReference = new \Google_Service_Bigquery_TableReference();
      $tableReference->setDatasetId($this->getDataSet());
      $tableReference->setProjectId($this->bigQueryProject());
      $tableReference->setTableId($bqTableName);
      $this->_tableRefs[$bqTableName] = $tableReference;
    }
    return $this->_tableRefs[$bqTableName];
  }

  /**
   * Create the dataset for the current organisation
   *
   * @param string $description
   *
   * @throws \Exception
   */
  public function createDataset($description = null)
  {
    if(!$this->_dataSetCreated)
    {
      $dataSet = $this->getDataSet();
      $service = $this->getService();

      $this->_log("Creating dataset " . $dataSet);

      $datasetReference = new \Google_Service_Bigquery_DatasetReference();
      $datasetReference->setProjectId($this->bigQueryProject());
      $datasetReference->setDatasetId($dataSet);
      $dataset = new \Google_Service_Bigquery_Dataset();
      $dataset->setDatasetReference($datasetReference);
      if($description)
      {
        $dataset->setDescription($description);
      }
      $options = [];
      try
      {
        $service->datasets->insert(
          $this->bigQueryProject(),
          $dataset,
          $options
        );
      }
      catch(\Exception $e)
      {
        if(!stristr($e->getMessage(), 'Already Exists: Dataset'))
        {
          throw $e;
        }
      }
      $this->_dataSetCreated = true;
    }
  }

  /**
   * Create a new table for a given IBigQueryWriteable object
   *
   * @param IBigQueryWriteable $object
   *
   * @throws \Exception
   */
  public function createTableForObject(
    IBigQueryWriteable $object, $useTemplateTable = false
  )
  {
    $tableName = $object->getBigQueryTableName();
    if($useTemplateTable)
    {
      list(, $tableName,) = $this->_splitTableNameForTemplate($tableName);
    }

    $this->createTable(
      $tableName,
      get_class($object),
      $object->getBigQuerySchema()
    );
  }

  /**
   * Create a new table
   *
   * @param string $tableName
   * @param string $description
   * @param array  $fields Array of fields where each field is in this format:
   *                       ['name' => 'fieldName', 'type' => 'integer', 'mode' => 'required']
   *
   * @throws \Exception
   */
  public function createTable($tableName, $description, $fields)
  {
    if(!isset($this->_createdTables[$tableName]))
    {
      $tableReference = $this->getTableReference($tableName);

      $this->_log("Creating table " . $this->getDataSet() . '.' . $tableName);

      $service = $this->getService();
      $tableSchema = new \Google_Service_Bigquery_TableSchema();
      $tableSchema->setFields($fields);
      $bqTable = new \Google_Service_Bigquery_Table();
      $bqTable->setDescription($description);
      $bqTable->setTableReference($tableReference);
      $bqTable->setSchema($tableSchema);
      $options = [];
      try
      {
        $service->tables->insert(
          $this->bigQueryProject(),
          $this->getDataSet(),
          $bqTable,
          $options
        );
      }
      catch(\Exception $e)
      {
        if(!stristr($e->getMessage(), 'Already Exists: Table'))
        {
          throw $e;
        }
      }
    }
    $this->_createdTables[$tableName] = true;
  }

  /**
   * @param string $tableName
   * @param bool   $noCache
   *
   * @return \Google_Service_Bigquery_TableSchema
   */
  public function getTableSchema($tableName, $noCache = false)
  {
    if($noCache || (!isset($this->_tableSchemas[$tableName])))
    {
      $table = $this->getService()->tables->get(
        $this->bigQueryProject(),
        $this->getDataSet(),
        $this->_trimDatasetFromTableName($tableName)
      );
      /** @var \Google_Service_Bigquery_TableSchema $schema */
      $this->_tableSchemas[$tableName] = $table->getSchema();
    }
    return $this->_tableSchemas[$tableName];
  }

  /**
   * @param string $tableName
   * @param bool   $detailed if true then include name, type and mode. Otherwise just name.
   *
   * @return string[]
   */
  public function getFieldsInTable($tableName, $detailed = false)
  {
    $schema = $this->getTableSchema($tableName);
    $fields = [];
    foreach($schema->getFields() as $field)
    {
      if($detailed)
      {
        $fields[] = [
          'name' => $field['name'],
          'type' => $field['type'],
          'mode' => $field['mode'],
        ];
      }
      else
      {
        $fields[] = $field['name'];
      }
    }
    return $fields;
  }

  /**
   * @param \Google_Service_Bigquery_Job $job
   * @param bool|false                   $async
   *
   * @return \Google_Service_Bigquery_Job
   * @throws \Exception
   */
  public function runJob(\Google_Service_Bigquery_Job $job, $async = false)
  {
    $res = $this->getService()->jobs->insert($this->bigQueryProject(), $job);
    if($async)
    {
      return $res;
    }
    else
    {
      $jobId = $res->getJobReference()->getJobId();
      return $this->waitForJob($jobId);
    }
  }

  /**
   * @param $jobId
   *
   * @return \Google_Service_Bigquery_Job
   * @throws \Exception
   */
  public function waitForJob($jobId)
  {
    while(true)
    {
      $res = $this->getService()->jobs->get($this->bigQueryProject(), $jobId);
      /** @var \Google_Service_Bigquery_JobStatus $status */
      $status = $res->getStatus();
      if($status->getState() != 'RUNNING')
      {
        /** @var \Google_Service_Bigquery_ErrorProto $err */
        $err = $status->getErrorResult();
        if($err)
        {
          // TODO: Deal with missing tables...?
          throw new \Exception(
            'Error running BigQuery job ' . $jobId . ' : '
            . json_encode(Objects::propertyValues($err))
          );
        }

        return $res;
      }
      usleep(500000);
    }
    throw new \Exception('Error waiting for BigQuery job ' . $jobId);
  }

  /**
   * Wait for multiple jobs to complete
   *
   * @param string[] $jobIds
   *
   * @return array
   */
  public function waitForJobs(array $jobIds)
  {
    $results = [];
    while(count($jobIds) > 0)
    {
      foreach($jobIds as $k => $jobId)
      {
        $res = $this->getService()->jobs->get(
          $this->bigQueryProject(),
          $jobId
        );
        /** @var \Google_Service_Bigquery_JobStatus $status */
        $status = $res->getStatus();
        if($status->getState() != 'RUNNING')
        {
          /** @var \Google_Service_Bigquery_ErrorProto $err */
          $err = $status->getErrorResult();
          $results[$jobId] = [
            'response' => $res,
            'status'   => $status->getState(),
            'error'    => $err
          ];
          unset($jobIds[$k]);
        }
      }
      usleep(500000);
    }
    return $results;
  }

  /**
   * Get a list of the tables in the current dataset
   *
   * @return string[]
   */
  public function listTables()
  {
    $allTables = $this->getAllTables();

    $tableNames = [];
    foreach($allTables as $table)
    {
      /** @var \Google_Service_Bigquery_TableListTables $table */
      $tableNames[] = $table->getTableReference()->getTableId();
    }
    return $tableNames;
  }

  /**
   * Get all of the tables in the current dataset
   *
   * @return \Google_Service_Bigquery_TableListTables[]
   * @throws \Exception
   */
  public function getAllTables()
  {
    $opts = [
      'maxResults' => 100,
    ];
    $allTables = [];
    $nextPageToken = true;
    while($nextPageToken)
    {
      $result = $this->getService()->tables->listTables(
        $this->bigQueryProject(),
        $this->getDataSet(),
        $opts
      );

      $tables = $result->getTables();
      $allTables = array_merge($allTables, $tables);

      $nextPageToken = $result->getNextPageToken();
      if($nextPageToken)
      {
        $opts['pageToken'] = $nextPageToken;
      }
    }
    return $allTables;
  }

  /**
   * Perform a callback for all tables in the dataset
   *
   * @param callable $callback
   */
  public function iterateTables(callable $callback)
  {
    $opts = [
      'maxResults' => 100,
    ];
    $nextPageToken = true;
    while($nextPageToken)
    {
      $result = $this->getService()->tables->listTables(
        $this->bigQueryProject(),
        $this->getDataSet(),
        $opts
      );

      /** @var \Google_Service_Bigquery_Table[] $tables */
      $tables = $result->getTables();

      foreach($tables as $table)
      {
        $tableId = $table->getTableReference()->getTableId();
        $callback($tableId);
      }

      $nextPageToken = $result->getNextPageToken();
      if($nextPageToken)
      {
        $opts['pageToken'] = $nextPageToken;
      }
    }
  }

  /**
   * Remove dataset from beginning of table if it has one
   *
   * @param string $tableName
   *
   * @return string
   * @throws \Exception
   */
  private function _trimDatasetFromTableName($tableName)
  {
    if(strpos($tableName, '.') !== false)
    {
      $parts = explode('.', $tableName, 2);
      if($parts[0] != $this->getDataSet())
      {
        throw new \Exception(
          'Incorrect dataset in table name: ' . $tableName
          . '. Expected dataset ' . $this->getDataSet()
        );
      }
      $tableName = $parts[1];
    }
    return $tableName;
  }

  /**
   * @param IBigQueryWriteable[] $rows          The rows to write
   * @param \callable|null       $onError       Called when there is a fatal error writing to a table.
   *                                            $onError($tableName, $dataKeys)
   * @param \callable|null       $onSuccess     Called when a table's data has been written successfully
   *                                            $onSuccess($tableName, $dataKeys)
   */
  public function writeWithRetries(
    array $rows, $onError = null, $onSuccess = null
  )
  {
    $groupedRows = $this->_groupRowsByTable($rows);
    $rowsToWrite = $groupedRows;

    $remainingAttempts = $totalAttempts = 5;

    while(($remainingAttempts > 0) && (count($rowsToWrite) > 0))
    {
      $remainingAttempts--;
      $attemptNum = $totalAttempts - $remainingAttempts;

      if($attemptNum > 1)
      {
        $this->_debug("Retrying write. Attempt number " . $attemptNum);
      }

      $errors = $this->writeBatched($rowsToWrite);

      $needDelay = false;
      $rowsToWrite = [];
      foreach($groupedRows as $tableName => $data)
      {
        $tags = array_keys($data);
        $errorKey = $tableName;

        if(isset($errors[$errorKey]))
        {
          $error = $errors[$errorKey];
          $msg = $error instanceof \Google_Service_Exception
            ? $error->getMessage() : $error;

          $this->_debug(
            "WARNING: Error writing to table " . $tableName
            . " on attempt " . $attemptNum . " of " . $totalAttempts
            . " : " . $msg
          );

          $errorState = $this->_handleTableError($error, $data);

          switch($errorState)
          {
            case self::ERR_FATAL:
              $this->_debug(
                'ERROR: Fatal error writing to table ' . $tableName
                . ' : ' . $msg
              );
              if($onError)
              {
                $onError($tableName, $tags);
              }
              break;
            case self::ERR_DELAYED_RETRY:
              $needDelay = true;
              // break intentionally missing
            case self::ERR_RETRY:
              // make sure we retry after this error
              /*if($remainingAttempts < 1)
              {
                $remainingAttempts++;
              }*/
              // break intentionally missing
            default:
              if($remainingAttempts < 1)
              {
                if($onError)
                {
                  $onError($tableName, $tags);
                }
                throw new \RuntimeException($msg);
              }
              $rowsToWrite[$tableName] = $data;
              break;
          }
        }
        else
        {
          if($onSuccess)
          {
            $onSuccess($tableName, $tags);
          }
        }
      }

      if(count($rowsToWrite) < 1)
      {
        break;
      }

      // Delay retries
      if($needDelay)
      {
        // Long delay after creating tables etc.
        sleep(10);
      }
      else
      {
        // backoff for increasing multiples of 500ms
        usleep($attemptNum * 500000);
      }
    }
  }

  /**
   * @param IBigQueryWriteable[] $rows
   *
   * @return array
   */
  protected function _groupRowsByTable(array $rows)
  {
    $grouped = [];
    foreach($rows as $key => $row)
    {
      $tableName = $row->getBigQueryTableName();
      if(isset($grouped[$tableName]))
      {
        $grouped[$tableName][$key] = $row;
      }
      else
      {
        $grouped[$tableName] = [$key => $row];
      }
    }
    return $grouped;
  }

  /**
   * @param IBigQueryWriteable[] $items            An array of items to write
   *                                               to BigQuery
   * @param bool                 $autoCreateTables If this is true then
   *                                               datasets and tables will be
   *                                               auto-created if they do not
   *                                               exist. If this happens the
   *                                               writes will be NOT retried
   *                                               after creating the tables.
   * @param callable|null        $onError          Function to call for each
   *                                               set of errors. Called with
   *                                               arguments ($tableName,
   *                                               $tags, $error)
   * @param callable|null        $onSuccess        Function to call for each
   *                                               set of successfil writes.
   *                                               Called with arguments
   *                                               ($tableName, $tags)
   *
   * @retun string[] Errors encountered during the operation, indexed by table
   *        name
   */
  public function writeTemplatedBatch(
    array $items, $autoCreateTables = false,
    callable $onError = null, callable $onSuccess = null
  )
  {
    $groupedItems = $this->_groupRowsByTable($items);
    if(count($groupedItems) < 1)
    {
      return [];
    }

    $startTime = microtime(true);
    $totalRows = count($items);

    $client = $this->getClient();
    $service = $this->getService();
    $dataSet = $this->getDataSet();

    // Build the batch of requests to send to BigQuery
    $responses = [];
    $errors = [];
    $client->setUseBatch(true);
    try
    {
      $batch = new \Google_Http_Batch($client);

      foreach($groupedItems as $fullTableName => $rows)
      {
        list(, $templateTable, $suffix) = $this->_splitTableNameForTemplate(
          $fullTableName
        );

        $bqRows = [];
        foreach($rows as $row)
        {
          $bqRow = new \Google_Service_Bigquery_TableDataInsertAllRequestRows();
          $bqRow->setJson($this->_serializeForBigQuery($row));
          $bqRow->setInsertId($row->getBigQueryInsertId());
          $bqRows[] = $bqRow;
        }

        $request = new \Google_Service_Bigquery_TableDataInsertAllRequest();
        $request->setKind('bigquery#tableDataInsertAllRequest');
        $request->setTemplateSuffix($suffix);
        $request->setRows($bqRows);
        $options = [];
        /** @var \Google_Http_Request $insertReq */
        $insertReq = $service->tabledata->insertAll(
          $this->bigQueryProject(),
          $dataSet,
          $templateTable,
          $request,
          $options
        );
        $batch->add($insertReq, $fullTableName);
      }

      $responses = $batch->execute();
    }
    finally
    {
      $client->setUseBatch(false);
    }
    $duration = microtime(true) - $startTime;

    $errorRows = 0;
    foreach(array_keys($groupedItems) as $tableName)
    {
      if(isset($responses['response-' . $tableName]))
      {
        $response = $responses['response-' . $tableName];

        if($response instanceof \Google_Service_Exception)
        {
          $errors[$tableName] = $response->getMessage();
        }
        else if($response instanceof \Google_Service_Bigquery_TableDataInsertAllResponse)
        {
          $insertErrors = $response->getInsertErrors();
          if(!empty($insertErrors))
          {
            $msg = $this->_makeErrorsMsg($insertErrors);
            $this->_log(
              'Errors inserting into table ' . $dataSet . '.' . $tableName
              . ': ' . $msg
            );
            $errors[$tableName] = $msg;
          }
        }
        else
        {
          $errors[$tableName] = 'Unknown response type: '
            . get_class($response);
        }
      }
      else
      {
        $errors[$tableName] = 'No response from BigQuery';
      }

      $tags = array_keys($groupedItems[$tableName]);
      if(isset($errors[$tableName]))
      {
        $errorRows += count($tags);
        if($onError)
        {
          $onError($tableName, $tags, $errors[$tableName]);
        }

        if($autoCreateTables)
        {
          $this->_handleTableError(
            $errors[$tableName],
            $groupedItems[$tableName],
            true
          );
        }
      }
      else if($onSuccess)
      {
        $onSuccess($tableName, $tags);
      }
    }

    $this->_debug(
      'Wrote ' . ($totalRows - $errorRows) . '/' . $totalRows
      . ' rows in ' . round($duration * 1000) . 'ms'
    );

    return $errors;
  }

  /**
   * @param IBigQueryWriteable[][] $groupedRows The rows to insert grouped by table name
   *
   * @return string[]|\Google_Service_Exception[] An array of errors encountered during the insert, indexed
   *                  by table name
   */
  public function writeBatched(array $groupedRows)
  {
    // Remove empty datasets
    foreach($groupedRows as $tableName => $rows)
    {
      if(count($rows) < 1)
      {
        unset($groupedRows[$tableName]);
      }
    }
    // Bail out if there is nothing to write
    if(count($groupedRows) < 1)
    {
      return [];
    }

    $startTime = floor(microtime(true) * 1000);

    $client = $this->getClient();
    $service = $this->getService();
    $dataSet = $this->getDataSet();

    $client->setUseBatch(true);
    $batch = new \Google_Http_Batch($client);

    try
    {
      // list of tables that were written to
      $tableList = [];
      $totalRows = $writtenRows = 0;
      foreach($groupedRows as $tableName => $rows)
      {
        /** @var IBigQueryWriteable[] $rows */
        $request = new \Google_Service_Bigquery_TableDataInsertAllRequest();
        $request->setKind('bigquery#tableDataInsertAllRequest');
        $numRows = count($rows);
        $totalRows += $numRows;
        $tableList[$tableName] = true;

        $bqRows = [];
        foreach($rows as $queuedRow)
        {
          $row = new \Google_Service_Bigquery_TableDataInsertAllRequestRows();
          $row->setJson($this->_serializeForBigQuery($queuedRow));
          $row->setInsertId($queuedRow->getBigQueryInsertId());
          $bqRows[] = $row;
        }

        $request->setRows($bqRows);
        $options = [];
        /** @var \Google_Http_Request $insertReq */
        $insertReq = $service->tabledata->insertAll(
          $this->bigQueryProject(),
          $dataSet,
          $tableName,
          $request,
          $options
        );
        $batch->add($insertReq, $tableName);
      }

      $responses = $batch->execute();
    }
    finally
    {
      $client->setUseBatch(false);
    }

    $errors = [];
    foreach(array_keys($tableList) as $tableName)
    {
      if(isset($responses['response-' . $tableName]))
      {
        $response = $responses['response-' . $tableName];

        if($response instanceof \Google_Service_Exception)
        {
          $errors[$tableName] = $response->getMessage();
        }
        else if($response instanceof \Google_Service_Bigquery_TableDataInsertAllResponse)
        {
          $insertErrors = $response->getInsertErrors();
          if(!empty($insertErrors))
          {
            $msg = $this->_makeErrorsMsg($insertErrors);
            $this->_debug(
              'Errors inserting into table ' . $dataSet . '.' . $tableName . ': '
              . $msg
            );
            $errors[$tableName] = $msg;
          }
          else
          {
            $writtenRows += count($groupedRows[$tableName]);
          }
        }
        else
        {
          $errors[$tableName] = 'Unknown response type: ' . get_class(
              $response
            );
        }
      }
      else
      {
        $errors[$tableName] = 'No response from BigQuery';
      }
    }

    $duration = floor(microtime(true) * 1000) - $startTime;

    /*$this->_debug(
      'Wrote ' . $writtenRows . ' of ' . $totalRows . ' rows to '
      . count($groupedRows) . ' table(s) in ' . $duration . ' ms with '
      . count($errors) . ' error(s)'
    );*/

    return $errors;
  }

  /**
   * Split a table name into its template table and suffix
   *
   * @param string $tableName
   *
   * @return string[]
   * @throws \Exception
   */
  protected function _splitTableNameForTemplate($tableName)
  {
    preg_match('/^(.*)(_[0-9]{8})$/', $tableName, $matches);
    if(count($matches) != 3)
    {
      throw new \Exception(
        'Table name not suitable for using template: ' . $tableName
      );
    }
    // [fullName, prefix, suffix]
    return [$tableName, $matches[1], $matches[2]];
  }

  /**
   * Handle errors from inserts - create missing datasets and tables if
   * required and return the appropriate error state
   *
   * @param string|\Google_Service_Exception $errorMsg
   * @param IBigQueryWriteable[]             $data
   *
   * @return int
   */
  protected function _handleTableError($errorMsg, array $data, $useTemplateTable = false)
  {
    $result = self::ERR_UNKNOWN;

    if($errorMsg instanceof \Google_Service_Exception)
    {
      if($errorMsg->allowedRetries() == 0)
      {
        $result = self::ERR_FATAL;
      }
    }
    else
    {
      /** @var IBigQueryWriteable $firstObj */
      $firstObj = reset($data);
      try
      {
        if(stristr($errorMsg, 'Not found: Dataset'))
        {
          $this->createDataset();
          $this->createTableForObject($firstObj, $useTemplateTable);
          $result = self::ERR_DELAYED_RETRY;
        }
        else if(stristr($errorMsg, 'Not found: Table'))
        {
          $this->createTableForObject($firstObj, $useTemplateTable);
          $result = self::ERR_DELAYED_RETRY;
        }
      }
      catch(\Exception $e)
      {
        $this->_log(
          "ERROR: (" . $e->getCode() . ") " . $e->getMessage() . "\n"
          . $e->getTraceAsString()
        );
        $result = self::ERR_FATAL;
      }
    }
    return $result;
  }

  protected function _serializeForBigQuery(IBigQueryWriteable $row)
  {
    $bqData = [];
    $rowData = $row->getBigQueryData();
    $schema = $row->getBigQuerySchema();
    foreach($schema as $field)
    {
      $fieldName = $field['name'];
      $value = isset($rowData[$fieldName]) ? $rowData[$fieldName] : '';
      $bqData[$fieldName] = $this->_serializeField(
        $value,
        $field['type'],
        $field['name']
      );
    }

    $rowTimestamp = $row->getBigQueryRowTimestamp();
    if($rowTimestamp)
    {
      $bqData['rowTimestamp'] = $rowTimestamp;
    }
    return $bqData;
  }

  /**
   * @param $value
   * @param $fieldType
   * @param $fieldName
   *
   * @return bool|float|int|string
   * @throws \Exception
   */
  protected function _serializeField($value, $fieldType, $fieldName)
  {
    switch($fieldType)
    {
      case 'integer':
      case 'timestamp':
        $tidyData = (int)$value;
        break;
      case 'float':
        $tidyData = (float)$value;
        break;
      case 'boolean':
        $tidyData = (bool)$value;
        break;
      case 'string':
        $tidyData = (string)$value;
        break;
      default:
        throw new \Exception(
          "Unable to serialize data for BigQuery: Unknown type '"
          . $fieldType . "' for field '" . $fieldName . "'"
        );
    }
    return $tidyData;
  }

  /**
   * @param \Google_Service_Bigquery_TableDataInsertAllResponseInsertErrors[] $insertErrors
   *
   * @return string
   */
  private function _makeErrorsMsg(array $insertErrors)
  {
    $insertMessages = [];
    foreach($insertErrors as $insertError)
    {
      /** @var \Google_Service_Bigquery_TableDataInsertAllResponseInsertErrors $insertError */
      $messages = [];
      foreach($insertError as $error)
      {
        /** @var \Google_Service_Bigquery_ErrorProto $error */
        $messages[] = sprintf(
          '[reason: %s, location: %s, message: %s]',
          $error->getReason(),
          $error->getLocation(),
          $error->getMessage()
        );
      }
      $insertMessages[] = sprintf(
        "Index %d: %s",
        $insertError->getIndex(),
        implode(', ', $messages)
      );
    }
    return implode("\n", $insertMessages);
  }
}
