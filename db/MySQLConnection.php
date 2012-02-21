<?php
//
// Copyright 2011 1889 Labs (contact: chris@courage-my-friend.org)
//    
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

  
  function connect( $host, $database, $user, $pass )
  {
    if( $connection = mysql_connect($host, $user, $pass) )
    {
      if( !mysql_select_db($database, $connection) )
      {
        mysql_close($connection);
        $connection = 0;
      }
      
      if( $connection )
      {
        $connection = new MySQLConnection($connection, $database);
      }
    }
    
    return $connection;
  }
  
  class MySQLConnection
  {
    function __construct( $connection, $database_name )
    {
      $this->connection    = $connection;
      $this->database_name = $database_name;
      $this->print_queries = false;
      $this->last_query    = "";
    }
    
    //
    // Executes a statement (with possible parameters) and returns the number of 
    // affected rows (or false if there was an error). Supports the same parameter
    // arrangements as format().
    
    function execute()
    {
      $args = func_get_args();
      if( mysql_query($this->postprocess($this->format($args)), $this->connection) )
      {
        return mysql_affected_rows($this->connection);
      }
      else
      {
        $this->report_error();
      }

      return false;
    }
    
    
    //
    // Executes an insert statement and returns the ID of the record inserted.
    // Supports the same parameter arrangements as format().
    
    function insert()
    {
      $args = func_get_args();
      return $this->execute($args) ? $this->last_insert_id() : 0;
    }
    
    
    //
    // Returns the last auto_increment value created.
    
    function last_insert_id()
    {
      return $this->query_value("id", 0, "SELECT LAST_INSERT_ID() as id");
    } 
    

    //
    // Executes a query (with possible parameters) and returns the resource handle
    // for direct retrieval. Supports the same parameter arrangements as format().
    
    function query()
    {
      $args = func_get_args();
      if( $handle = mysql_query($this->postprocess($this->format($args)), $this->connection) )
      {
        return new Results($handle);
      }      
      else
      {
        $this->report_error();
      }
      return null;
    }
    
    
    //
    // Executes a query (with possible parameters) and returns the whole set in an array 
    // of objects. Supports the same parameter arrangements as format().
    
    function query_all()
    {
      $all  = array();
      $args = func_get_args();
      if( $results = $this->query($args) )
      {
        while( $row = $results->fetch() )
        {
          $all[] = $row;
        }
        
        $results->close();        
      }
      
      return $all;
    }
      
        
    //
    // Executes a query (with possible parameters) and returns the first row.
    // Supports the same parameter arrangements as format().
    
    function query_first()
    {
      $row  = null;
      $args = func_get_args();
      if( $results = $this->query($args) )
      {
        $row = $results->fetch();
        $results->close();
      }
      
      return $row;
    }
    
    
    //
    // Executes a query (with possible parameters) and returns true if any 
    // records are returned. Supports the same parameter arrangements as format().
    
    function query_exists()
    {
      $args = func_get_args();
      $first = $this->query_first($args);
      return !empty($first);
    }
    
    
    //
    // Executes a query (with possible parameters) and returns a value from the first
    // row. The first parameter is the field name, and the second parameter
    // is the default return value if no row is found. After that, pass the same 
    // parameters you would to format().
    
    function query_value()
    {
      $args    = func_get_args();
      $field   = array_shift($args);
      $default = array_shift($args);
      if( $row = $this->query_first($args) )
      {
        return $row->$field;
      }

      return $default;
    }
    
    
    //
    // Executes a query and returns all values from a single column of the results. The first 
    // parameter is the column name. After that, pass the same parameters you would to format().
    
    function query_column()
    {
      $args   = func_get_args();
      $field  = array_shift($args);
      $column = array();
      if( $results = $this->query($args) )
      {
        while( $row = $results->fetch() )
        {
          $column[] = $row->$field;
        }
        
        $results->close();
      }
      
      return $column;
    }
    

    //
    // Executes a query and returns a map of one column to another.  The first 
    // parameter is the key column, and the second is the value column. After that,
    // pass the same parameters you would to format().
    
    function query_map()
    {
      $args        = func_get_args();
      $key_field   = array_shift($args);
      $value_field = array_shift($args);
      
      $map = array();
      if( $results = $this->query($args) )
      {
        while( $row = $results->fetch() )
        {
          $map[$row->$key_field] = $row->$value_field;
        }
        
        $results->close();
      }
      
      return $map;
    }
    

    //
    // Builds a nested (tree) structure from a sorted, flat record set, based on a program you 
    // supply. Useful for when you join across a sequence of one-to-many relationship, and want a
    // nested data structure back out. 
    // 
    // The program you supply is simply an associative array of levels, with each level listing a 
    // key field and one or more value fields for that level. The level key (in the outermost 
    // associative array) indicates the collection of child objects, once built.
    //
    // Example:
    //   $query = "
    //     SELECT a.id, a.name, b.id as room_id, b.room_name, b.size, c.id as item_id, c.item
    //     FROM a
    //     JOIN b on b.a_id = a.id
    //     JOIN c on c.b_id = b.id
    //     ORDER BY a.id, room_id, item_id
    //   ";
    //   
    //   $program = array(
    //       0       => array("name", "id", "name", "age")      // The top-level map of name to record
    //     , "rooms" => array("room_name", "room_id", "size")   // An map of objects to appear in top->rooms
    //     , "items" => array(null, "item")                     // An array of strings to appear in top->room->items
    //   );
    // 
    //   $structure = $db->query_structure($program, $query);
    
    function query_structure()
    {
      $args    = func_get_args();
      $args    = $this->extract($args);
      $program = array_shift($args);
      
      //
      // We are sort of knitting the flat record set back into a tree. $top is the real
      // result set to be returned, while the working edge of all (nested) levels is 
      // maintained in $lasts.
      
      
      $top   = array();
      $lasts = array();
      $level_map = array_keys($program);
      foreach( $this->query_all($args) as $record )
      {
        //
        // First, pop off anything that is finished. 

        foreach( $level_map as $level_index => $level_name )
        {
          if( count($lasts) > $level_index )
          {
            foreach( $program[$level_name] as $field )
            {
              if( $field && $lasts[$level_index]->$field != $record->$field )
              {
                while( count($lasts) > $level_index )
                {
                  array_pop($lasts);
                }

                break 2;
              }
            }
          }
        }


        //
        // Next, create objects along the current record.

        for( $level_index = count($lasts); $level_index < count($level_map); $level_index++ )
        {
          //
          // Get the level instructions. The first name is the key, the rest fields.

          $level  = $program[$level_map[$level_index]];
          $key    = $level[0];
          $fields = array_slice($level, 1);

          //
          // Create an object to hold this branches' data and fill it with data from this record.

          if( $level_index + 1 >= count($level_map) && count($fields) == 1 )
          {
            foreach( $fields as $field )
            {
              $object = $record->$field;
              break;
            }
          }
          else
          {
            $object = new stdClass;
            $object->$key = $record->$key;
            
            if( $level_index + 1 < count($level_map) )
            {
              $child_container_name = $level_map[$level_index + 1];
              $object->$child_container_name = array();
            }

            foreach( $fields as $field )
            {
              $object->$field = $record->$field;
            }
          }
          
          //
          // Add the object to its container and make sure it can be found for the next level and 
          // future records.

          $container =& $top;
          if( $level_index )
          {
            $parent_container_name = $level_map[$level_index];
            $container =& $lasts[$level_index - 1]->$parent_container_name;
          }

          if( is_string($key) )
          {
            $container[$record->$key] = $object;
          }
          else
          {
            $container[] = $object;
          }

          $lasts[$level_index] = $object;
        }
      }
      
      return $top;
    }
    

    //
    // Formats a query, by replacing all ? markers with the properly escaped and
    // quoted parameters.
    //
    // Note that you can pass parameters in just about any way that is convenient:
    //  - a parameter list
    //  - an array
    //  - a query and an array of parameters
    //  - etc.
    
    function format()
    {
      $args  = func_get_args();
      $args  = $this->extract($args);
      $query = array_shift($args);
      $args  = $this->extract($args);
      
      if( empty($args) )
      {
        return $query;
      }
      else
      {
        $safe = array();
        foreach( $args as $arg )
        {
          if( is_null($arg) )
          {
            $safe[] = "null";
          }
          elseif( is_string($arg) )
          {
            $safe[] = sprintf("'%s'", $this->escape($arg));
          }
          elseif( is_bool($arg) )
          {
            $safe[] = $arg ? 1 : 0;
          }
          else
          {
            $safe[] = 0 + $arg;
          }
        }

        return vsprintf(str_replace("?", "%s", str_replace("%", "%%", $query)), $safe);
      }
    }
    
    function format_time( $time )
    {
      return date("Y-m-d H:i:s", $time);
    }
    
    function format_date( $time )
    {
      return date("Y-m-d", $time);
    }
    
    function print_queries( $format = "pre" )
    {
      $this->print_queries = $format;
    }
      
    function close()
    {
      mysql_close($this->connection);
      $this->connection = 0;
    }
    
    function escape( $string )
    {
      return mysql_real_escape_string($string, $this->connection);
    }
    
    function extract( $array )
    {
      while( count($array) == 1 && is_array($array[0]) )
      {
        $array = $array[0];
      }
      
      return $array;
    }
    
    function postprocess( $query )
    {
      if( empty($query) )
      {
        trigger_error("did you forget to pass a query?", E_USER_ERROR);
      }
      
      $this->print_query($query);
      $this->last_query = $query;
      return $query;
    }
    
    function report_error()
    {
      if( $errno = mysql_errno($this->connection) )
      {
        trigger_error(sprintf("%d: %s", $errno, mysql_error($this->connection)), E_USER_ERROR);
      }
    }
    
    function print_query( $query )
    {      
      switch( $this->print_queries )
      {
        case "pre":
          print code_block($query, true, ".debug");
          break;
        case "comment":
          print "<!-- ";
          print text($query);
          print " -->\n";
      }
      
      return $query;
    }
  }
  
  
  class Results
  {
    function __construct( $handle )
    {
      $this->handle = $handle;
    }
    
    function count()
    {
      return mysql_num_rows($this->handle);
    }
    
    function fetch()
    {
      return mysql_fetch_object($this->handle);
    }
    
    function close()
    {
      mysql_free_result($this->handle);
      $this->handle = 0;
    }
  }

    
