<?php
namespace Packaged\BigQuery;

class BigQueryType
{
  /** 2 MB UTF-8 encoded string */
  const STRING = 'string';
  /** 64-bit signed integer */
  const INTEGER = 'integer';
  /** Double-precision floating-point format */
  const FLOAT = 'float';
  /** true or false (case-insensitive) */
  const BOOLEAN = 'boolean';
  /** A collection of one or more other fields */
  const RECORD = 'record';
  /** YYYY-MM-DD HH:MM:SS or UNIX timestamp with up to 6 decimal places */
  const TIMESTAMP = 'timestamp';

  function isValidType($type)
  {
    switch($type)
    {
      case self::STRING:
      case self::INTEGER:
      case self::FLOAT:
      case self::BOOLEAN:
      case self::RECORD:
      case self::TIMESTAMP:
        return true;
    }
    return false;
  }
}
