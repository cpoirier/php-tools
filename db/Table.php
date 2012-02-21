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


  //
  // Captures the definition of a table and provides services thereon.
  
  class Table
  {
    function __construct( $name, $id_field = "id", $id_is_autoincrement = true )
    {
      $this->name     = $name;
      $this->id_field = $id_field;
      $this->types    = array();      // $field => string|boolean|real|integer
      $this->defaults = array();      // $field => default value
      $this->filters  = array();      // $field => list of filters
      $this->checks   = array();      // list of checks, first element of each is a field or list of fields, second is check name
      $this->current  = null;      
      $this->custom_filters = array();
      $this->maps     = array();

      $this->id_is_autoincrement = $id_is_autoincrement;
      if( !empty($id_field) )
      {
        $this->define_field($id_field, 0);
      }
    }
    
    function define_field( $field, $default /* type override ("as_<type>"), filters, checks */ )
    {
      $type = null;
      $this->defaults[$field] = $default;
      $this->filters[$field]  = array();

          if( is_string($default)  ) { $type = "string" ; }
      elseif( is_bool($default)    ) { $type = "boolean"; }
      elseif( is_real($default)    ) { $type = "real"   ; }
      elseif( is_integer($default) ) { $type = "integer"; }
      
      if( func_num_args() > 2 )
      {
        $args  = func_get_args();
        $extra = array_slice($args, 2);
        if( strpos($extra[0], "as_") === 0 )
        {
          $type = substr(array_shift($extra), 3);
        }
        
        foreach( $extra as $item )
        {
          is_array($item) or $item = array($item);
          array_unshift($item, $field);
          call_user_func_array(array($this, strpos($item[0], "_to_") ? "add_filter" : "add_check"), $item); 
        }
      }
      
      $this->types[$field] = $type;
      array_unshift($this->filters[$field], "to_$type");
    }
    
    function add_filter( /* $field, $filter, ... */ )
    {
      $args  = func_get_args();
      $field = array_shift($args);
      $this->filters[$field][] = $args;
    }
    
    function add_check( /* $field, $filter, ... */ )
    {
      $args = func_get_args();
      $this->checks[] = $args;
    }
    
    function add_custom_filter( $callback )
    {
      $this->custom_filters[] = $callback;
    }
    
    function has_field( $name )
    {
      foreach( $this->types as $field => $type )
      {
        if( $field == $name )
        {
          return true;
        }
      }
      
      return false;
    }
    


    //===========================================================================================

    function find( $db, $criteria )
    {
      $criteria = $this->make_criteria($db, $criteria);
      $table = $this->name($db);
      $this->last_query = sprintf("SELECT $this->id_field FROM $table WHERE $criteria");
      return $db->query_value($this->id_field, 0, $this->last_query);
    }

    function load( $db, $criteria, $order = null )
    {                            
      return $db->query_first($this->make_query($db, $criteria, $order) . " LIMIT 1");
    }
    
    function load_all( $db, $criteria, $order = null, $fields = array() )
    {                            
      return $db->query_all($this->make_query($db, $criteria, $order));
    }
    
    function map( $db, $value_field, $criteria = null, $order = null, $key_field = null )
    {
      if( is_null($key_field) )
      {
        $key_field = $this->id_field;
      }

      $fields = array($key_field, "accessor_map_key" => $value_field);
      $query  = $this->make_query($db, $criteria, $order, $fields);
      if( array_key_exists($query, $this->maps) )
      {
        return $this->maps[$query];
      }
      else
      {
        $map = $db->query_map($key_field, "accessor_map_key", $query);
        if( count($map) < 200 )
        {
          $this->maps[$query] = $map;
        }
        return $map;
      }
    }
    
    function make_query( $db, $criteria = null, $order = array(), $fields = array() )
    {
      $table    = $this->name($db);
      $select   = is_string($fields) ? $fields : $this->make_select_list($db, $fields);
      $criteria = $this->make_criteria($db, $criteria);
      $order_by = $this->make_order_by($db, $order);
      $this->last_query = "SELECT $select FROM $table WHERE $criteria $order_by";
      
      return $this->last_query;
    }
    

    function save( $db, $id, $fields, $skip_checks = false, $criteria = null )
    {
      if( empty($fields) )
      {
        if( $id )
        {
          return $this->delete($db, $id, null, $criteria);          
        }
        else
        {
          return array("no_change");
        }
      }
      elseif( $id )
      {
        return $this->update($db, $id, $fields, $skip_checks, $criteria);
      }
      else
      {
        return $this->insert($db, $id, $fields, $skip_checks);
      }
    }


    function delete( $db, $id, $fields = array(), $criteria = null )
    {
      $table = $this->name($db);
      
      if( $db->execute("DELETE FROM $table WHERE $this->id_field = ?" . ($criteria ? " and $criteria" : ""), $id) == 1 )
      {
        return array("deleted", $id);
      }
      
      return array("no_change");
    }
    

    function insert( $db, $id, $fields, $skip_checks = false )
    {
      $table = $this->name($db);
      
      //
      // First, filter and check the data.
      
      $fields = $this->filter($db, $id, $fields, true);
      if( !$skip_checks )
      {
        $result = $this->check($db, $id, $fields);
        if( !empty($result) )
        {
          return $result;
        }
      }
      
      //
      // If we are manually incrementing the numeric key for the table, do that first.
      
      if( !$this->id_is_autoincrement )
      {
        $db->execute("LOCK TABLES $table");
        $id = $db->query_value("id", 0, "SELECT ifnull(MAX(s.$field), 0) + 1 FROM $table s");
      }
      
      //
      // Generate the name and values clauses for the query.
      
      $names  = array();
      $values = array();
      foreach( $fields as $field => $value )
      {
        if( isset($this->types[$field]) )
        {
          if( $field == $this->id_field )
          {
            if( !$this->id_is_autoincrement )
            {
              $names[]  = "`$field`";
              $values[] = $id;
            }
          }
          else
          {
            $names[]  = "`$field`";
            $values[] = $db->format("?", $value); 
          }
        }
      }
      
      //
      // Assemble and run the query.
      
      $query = sprintf("INSERT INTO $table (%s) VALUES (%s)", implode(", ", $names), implode(", ", $values));
      $changed = $db->execute($query);
      
      //
      // Unlock the table, if necessary.
      
      if( !$this->id_is_autoincrement )
      {
        $db->execute("UNLOCK TABLES $table");
      }

      //
      // Return appropriately.
      
      if( $changed == 1 )
      {
        if( $this->id_is_autoincrement )
        {
          return array("added", $db->last_insert_id());
        }
        else
        {
          return array("added", $id);
        }
      }
      else
      {
        return array("failed", $changed);
      }
    }
    
    
    function update( $db, $id, $fields, $skip_checks = false, $criteria = null )
    {
      $table = $this->name($db);
      
      //
      // First, filter and check the data. We'll need any data missing from the fields,
      // in order to properly run checks. As a result, things get a little messy.

      if( $skip_checks )
      {
        $fields = $this->filter($db, $id, $fields);
      }
      else
      {
        $updates  = $this->filter($db, $id, $fields);
        $existing = $this->load($db, $id);
        foreach( $this->types as $field => $type )
        {
          if( !isset($fields[$field]) )
          {
            $fields[$field] = $existing->$field;
          }
        }

        $fields = $this->filter($db, $id, $fields);
        $result = $this->check($db, $id, $fields);
        if( !empty($result) )
        {
          return $result;
        }
        
        $fields = $updates;
      }
      

      //
      // Generate the set clauses for the query.
      
      $sets = array();
      foreach( $fields as $field => $value )
      {
        if( isset($this->types[$field]) )
        {
          if( $field != $this->id_field )
          {
            $sets[] = $db->format("`$field` = ?", $value);
          }
        }
      }
      
      //
      // Assemble and run the query.
      
      $query = sprintf("UPDATE $table SET %s WHERE $this->id_field = %d %s", implode(", ", $sets), $id, (!empty($criteria) ? "and $criteria" : ""));
      $changed = $db->execute($query);
      if( $changed == 1 )
      {
        return array("updated", $id);
      }
      elseif( $changed == 0 )
      {
        return array("no_change");
      }
      else
      {
        return array("failed", $changed);
      }
    }
    


    //===========================================================================================
    
    
    function filter_to_string ( $value, $parameters ) { return "$value";     }
    function filter_to_boolean( $value, $parameters ) { return !!$value;     }
    function filter_to_real   ( $value, $parameters ) { return 0.0 + $value; }
    function filter_to_integer( $value, $parameters ) { return 0 + $value;   }
    function filter_to_time   ( $value, $parameters ) { return $value;       }
    function filter_to_date( $value, $parameters )
    {
      $time = is_numeric($value) ? $value : strtotime($value);
      return date("Y-m-d", $time);
    }
    function filter_to_datetime( $value, $parameters )
    {
      $time = is_numeric($value) ? $value : strtotime($value);
      return date("Y-m-d H:i:s", $time);
    }
    
    function filter_empty_to_null( $value, $parameters ) 
    {      
      return empty($value) ? null : $value;
    }
    
    function filter_epoch_to_null( $value, $parameters )
    {
      return ($value == 0 || $value == "1969-12-31" || $value == "1969-12-31 19:00:00") ? null : $value;
    }
    
    function filter_to_lowercase( $value, $parameters ) { return strtolower($value); }
    function filter_to_uppercase( $value, $parameters ) { return strtoupper($value); }
    function filter_to_ucwords  ( $value, $parameters ) { return ucwords($value);    }
    
    function filter_one_of( $value, $parameters ) 
    {
      $options = $parameters[0];
      return in_array($value, $options) ? $value : $options[0];
    }

    
    //===========================================================================================

    function check_read_only( $record, $subject, $id, $db, $parameters )
    {
      return false;
    }
    
    function check_not_null( $record, $subject, $id, $db, $parameters )
    {
      return array_key_exists($subject, $record) && !is_null($record[$subject]);
    }

    function check_not_empty( $record, $subject, $id, $db, $parameters )
    {
      return !empty($record[$subject]);
    }
    
    function check_not_epoch( $record, $subject, $id, $db, $parameters )
    {
      return !($record[$subject] == 0 || $record[$subject] == "1969-12-31" || $record[$subject] == "1969-12-31 19:00:00");
    }
    
    function check_unique( $record, $subject, $id, $db, $parameters )
    {
      $criteria = array();
      if( is_array($subject) )
      {
        $fields = $subject;
        foreach( $fields as $field )
        {
          $criteria[] = $db->format("`$field` = ?", $record[$field]);
        }
      }
      else
      {
        $field = $subject;
        $criteria[] = $db->format("`$field` = ?", $record[$field]);
      }
      
      $table = $this->name($db);
      $found = $db->query_value("id", 0, "SELECT $this->id_field as id FROM $table WHERE " . implode(" and ", $criteria));
      return ($found == 0 || $found == $id) ? true : $found;
    }
    
    function check_min_date( $record, $subject, $id, $db, $parameters )
    {
      $date = array_shift($parameters);
      return strtotime($record[$subject]) > strtotime($date);
    }
    
    function check_max_length( $record, $subject, $id, $db, $parameters )
    {
      return strlen($record[$subject]) <= $parameters[0];
    }
    
    function check_min_length( $record, $subject, $id, $db, $parameters )
    {
      return strlen($record[$subject]) >= $parameters[0];
    }
    
    function check_member_of( $record, $subject, $id, $db, $parameters )
    {
      list($referenced_table, $referenced_field) = $parameters;
      
      $criteria = array();
      if( is_array($subject) )
      {
        foreach( $subject as $index => $field )
        {
          $referenced_name = $referenced_field[$index];
          if( is_null($record[$field]) )
          {
            return true;
          }
          $criteria[] = $db->format("`$referenced_name` = ?", $record[$field]);
        }
      }
      else
      {
        $field = $subject;
        if( is_null($record[$field]) )
        {
          return true;
        }
        $criteria[] = $db->format("`$referenced_field` = ?", $record[$field]);
      }
      
      $table = isset($db->$referenced_table) ? $db->$referenced_table : $referenced_table;
      return $db->query_exists("SELECT * FROM $table WHERE " . implode(" and ", $criteria));
    }


    //===========================================================================================


    function name( $db )
    {
      $name = $this->name;
      return $db->$name;
    }

    
    function filter( $db, $id, $fields, $canonicalize = false )
    {
      if( $canonicalize )
      {
        foreach( $this->types as $field => $type )
        {
          if( !isset($fields[$field]) )
          {
            $fields[$field] = null;
          }
        }
      }
      
      foreach( $fields as $field => $value )
      {
        if( isset($this->filters[$field]) )
        {
          foreach( $this->filters[$field] as $parameters )
          {
            $filter = array_shift($parameters);
            $method = "filter_$filter";
            $fields[$field] = $this->$method($fields[$field], $parameters);    
          }
        }
      }
      
      foreach( $this->custom_filters as $custom_filter )
      {
        $fields = call_user_func($custom_filter, $fields);
      }
      
      return $fields;
    }
    
    
    function check( $db, $id, $fields )
    {
      foreach( $this->checks as $parameters )
      {
        $subject = array_shift($parameters);
        $check   = array_shift($parameters);
        $method  = "check_$check";
        $result  = $this->$method($fields, $subject, $id, $db, $parameters);
        
        if( $result !== true )
        {
          return array("failed_check", $check, $subject, $result);
        }
      }

      return null;
    }
    
    
    function make_select_list( $db, $fields = array() )
    {
      $list = array();
      if( empty($fields) )
      {
        foreach( array_keys($this->types) as $field )
        {
          $list[] = "`$field`";
        }
      }
      else
      {
        foreach( $fields as $name => $value )
        {
          if( is_numeric($name) )
          {
            $list[] = "`$value`";
          }
          elseif( is_numeric(strpos($value, "`")) || is_numeric(strpos($value, " ")) )
          {
            $list[] = sprintf("%s as %s", $value, $name);
          }
          else
          {
            $list[] = sprintf("`%s` as `%s`", $value, $name);
          }
        }
      }
      
      return implode(", ", $list);
    }


    function make_criteria( $db, $criteria )
    {
      if( empty($criteria) )
      {
        return "1 = 1";
      }
      if( is_numeric($criteria) )
      {
        $criteria = sprintf("`%s` = %d", $this->id_field, $criteria);
      }
      elseif( is_array($criteria) )
      {
        $pairs = array();
        foreach( $criteria as $field => $value )
        {
          $pairs[] = $db->format("`$field` = ?", $value);
        }
        $criteria = implode(" and ", $pairs);
      }
      
      return str_replace(" = null", " is null", $criteria);
    }


    function make_order_by( $db, $order )
    {
      $clause = "";
      
      if( !empty($order) )
      {
        $orderings = array();
        $spec  = "`%s` %s";
        foreach( $order as $key => $value )
        {
          $direction = "asc";
          if( is_numeric($key) )
          {
            $orderings[] = sprintf($spec, $value, $direction);
          }
          else
          {
            if( !$value || $value == "desc" || $value == "DESC" )
            {
              $direction = "desc";
            }
            
            $orderings[] = sprintf($spec, $key, $direction);
          }
        }
        $clause = " ORDER BY " . implode(", ", $orderings);
      }
      
      return $clause;
    }
    
  }
