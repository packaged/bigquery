<?php
namespace Packaged\BigQuery\Tests;

use Packaged\BigQuery\BigQueryType;
use PHPUnit\Framework\TestCase;

class BigQueryTypeTest extends TestCase
{
  public function typeProvider()
  {
    $types = [];
    foreach((new \ReflectionClass(BigQueryType::class))->getConstants() as $name => $value)
    {
      $types[$name] = [$value];
    }
    return $types;
  }

  /**
   * @dataProvider typeProvider
   */
  public function testEveryDefinedTypeIsValid(string $type)
  {
    $this->assertTrue(BigQueryType::isValidType($type));
  }

  public function testUnknownTypeIsInvalid()
  {
    $this->assertFalse(BigQueryType::isValidType('geography'));
    $this->assertFalse(BigQueryType::isValidType(null));
  }
}
