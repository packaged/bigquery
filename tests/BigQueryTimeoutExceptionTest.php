<?php
namespace Packaged\BigQuery\Tests;

use Packaged\BigQuery\BigQueryException;
use Packaged\BigQuery\BigQueryTimeoutException;
use PHPUnit\Framework\TestCase;

class BigQueryTimeoutExceptionTest extends TestCase
{
  public function testCarriesJobReference()
  {
    $previous = new \RuntimeException('cause');
    $e = new BigQueryTimeoutException('timed out', 7, $previous, 'SELECT 1', 'project', 'job', 'EU');

    $this->assertInstanceOf(BigQueryException::class, $e);
    $this->assertSame('timed out', $e->getMessage());
    $this->assertSame(7, $e->getCode());
    $this->assertSame($previous, $e->getPrevious());
    $this->assertSame('SELECT 1', $e->getQuery());
    $this->assertSame('project', $e->getProjectId());
    $this->assertSame('job', $e->getJobId());
    $this->assertSame('EU', $e->getLocation());
  }

  public function testDefaults()
  {
    $e = new BigQueryTimeoutException();
    $this->assertNull($e->getPrevious());
    $this->assertSame('', $e->getQuery());
    $this->assertSame('', $e->getJobId());
  }
}
