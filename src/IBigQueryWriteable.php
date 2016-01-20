<?php

namespace Packaged\BigQuery;

interface IBigQueryWriteable
{
  /**
   * @return string
   */
  public function getBigQueryTableName();

  /**
   * @return array Array of fields where each field is
   * ['name' => '', 'type' => '', 'mode' => '']
   */
  public function getBigQuerySchema();

  /**
   * @return array Array of [field => value]
   */
  public function getBigQueryData();

  /**
   * @return string
   */
  public function getBigQueryInsertId();

  /**
   * Get the rowTimestamp to write for this object.
   * If this returns null (or anything falsey) a rowTimestamp field will not be written.
   *
   * @return int|null
   */
  public function getBigQueryRowTimestamp();
}
