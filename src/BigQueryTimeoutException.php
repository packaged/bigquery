<?php
namespace Packaged\BigQuery;

use Throwable;

class BigQueryTimeoutException extends BigQueryException
{
  protected $_projectId;
  protected $_jobId;
  protected $_location;
  protected $_query;

  public function __construct(
    $message = "", $code = 0, Throwable $previous = null, $query = "", $projectId = "", $jobId = "", $location = ""
  )
  {
    parent::__construct(
      $message,
      $code,
      $previous
    );
    $this->_query = $query;
    $this->_projectId = $projectId;
    $this->_jobId = $jobId;
    $this->_location = $location;
  }

  public function getProjectId()
  {
    return $this->_projectId;
  }

  public function getJobId()
  {
    return $this->_jobId;
  }

  public function getLocation()
  {
    return $this->_location;
  }

  public function getQuery()
  {
    return $this->_query;
  }

}
