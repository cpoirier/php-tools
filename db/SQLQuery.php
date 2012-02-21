<?php
// =============================================================================================
// Schemaform
// A high-level database construction and programming layer.
//
// [Website]   http://schemaform.org
// [Copyright] Copyright 2004-2012 Chris Poirier
// [License]   Licensed under the Apache License, Version 2.0 (the "License");
//             you may not use this file except in compliance with the License.
//             You may obtain a copy of the License at
//             
//                 http://www.apache.org/licenses/LICENSE-2.0
//             
//             Unless required by applicable law or agreed to in writing, software
//             distributed under the License is distributed on an "AS IS" BASIS,
//             WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//             See the License for the specific language governing permissions and
//             limitations under the License.
// =============================================================================================



  class SQLQuery
  {
    function __construct( $table )
    {
      $this->fields           = array();
      $this->sources          = array();        // alias => SQLQuerySource
      $this->where_condition  = null;           
      $this->group_by_fields  = null;
      $this->having_condition = null;
      $this->order_by_fields  = null;
      $this->offset           = 0;
      $this->limit            = 0;
      $this->has_aggregates   = false;

      $this->add_source(new SQLQuerySource($table));
    }
    
    
    //
    // Projects some subset of fields from the underlying source(s). You can pass a list of names, 
    // or an associative array of mappings.
    
    function project( $mappings )
    {
      $result = empty($this->group_by_fields) ? clone $this : new static($this);
      
      if( !is_array($mappings) )
      {
        $mappings = func_get_args();
      }

      $fields = array();
      foreach( $mappings as $from => $to )
      {
        if( is_numeric($from) )
        {
          $from = $to;
        }

        $fields[$to] = $this->fields[$from];
      }
      
      $result->fields = $fields;

      return $result;
    }
    
    
    //
    // An alias for project().
    
    function select( $mappings )
    {
      if( !is_array($mappings) )
      {
        $mappings = func_get_args();
      }

      return $this->project($mappings);
    }
    
    
    //
    // Removes some subset of fields from the relation. You can pass a list of names or an array.
    
    function discard( $victims )
    {
      if( !is_array($victims) )
      {
        $victims = func_get_args();
      }
      
      return $this->project(array_diff(array_keys($this->fields), $victims));
    }
    
    
    //
    // Renames one or more fields. You can pass an associative array of mappings (from => to) or
    // a single pair of from and to. Original field order will be respected.
    
    function rename( $mappings )
    {
      if( !is_array($mappings) )
      {
        $args = func_get_args();
        $mappings = array($args[0] => $args[1]);
      }
      
      $ordered = array();
      foreach( $this->fields as $name => $ignored )
      {
        $ordered[$name] = array_key_exists($name, $mappings) ? $mappings[$name] : $name;
      }
      
      return $this->project($ordered);
    }
    
    
    //
    // Adds a prefix to the named fields. If you don't pass any fields, the prefix is added
    // to all fields.
    
    function prefix( $prefix )
    {
      $names = func_get_args(); array_shift($names);
      $ordered = array();
      foreach( $this->fields as $name => $ignored )
      {
        $ordered[$name] = (empty($names) || in_array($name, $names) ? sprintf("%s%s", $prefix, $name) : $name);
      }
      
      return $this->project($ordered);
    }
    

    //
    // Returns a query with restricted results.
    
    function where( $expression )
    {
      $copy = clone $this;
      return $copy->and_where($this->resolve_expression($expression));
    }
    
    
    //
    // Produces the natural join of this and the RHS Table or SQLQuery (ie. they will be joined on 
    // all common fields). As a checksum, you may pass the list of expected join fields, and an
    // error will be triggered if it doesn't match the actual relations. If you need a left join,
    // pass "left" as the first parameter.
    
    function natural_join( $rhs )
    {
      $args = func_get_args(); array_shift($args);
      $type = "";
      if( is_string($rhs) )
      {
        $type = $rhs;
        $rhs  = array_shift($args);
      }
      
      if( is_a($rhs, "Table") )
      {
        $rhs = new static($rhs);
      }
      
      $natural  = array_intersect(array_keys($this->fields), array_keys($rhs->fields));
      $expected = $args;
      if( !empty($expected) && count(array_intersect($natural, $expected)) != count($natural + $expected) )
      {
        $natural  = implode(", ", $natural);
        $expected = implode(", ", $expected);
        trigger_error("natural join of two relations does not meet expectations: [$expected] versus [$natural]", E_USER_ERROR);
        return null;
      } 
      
      $renames    = array();
      $conditions = array();
      $discards   = array();
      foreach( $natural as $name )
      {
        $rename         = "${name}__rhs";
        $renames[$name] = $rename; 
        $conditions[]   = "$name = ${rename}";
        $discards[]     = $rename;
      } 
      
      return $this->join($rhs->rename($renames), implode(" and ", $conditions), $type)->discard($discards);
    }
    
    function natural_left_join( $rhs )
    {
      $args = func_get_args();
      array_unshift($args, "left");
      return call_user_func_array(array($this, "natural_join"), $args);
    }


    function join( $rhs, $condition = null, $type = "" )
    {
      if( !is_a($rhs, "SQLQuery") )
      {
        $rhs = new SQLQuery($rhs);
      }
      
      $result = clone $this;
      if( count($rhs->sources) == 1 && empty($rhs->group_by_fields) && empty($rhs->offset) && empty($rhs->limit) && !$rhs->has_aggregates )
      {
        //
        // The RHS query is simple, and so its source can be merged into this one directly, saving a subtable.
        // We'll have to rename the table alias when importing the fields and any where condition.

        $join = new SQLQueryJoin($type, $rhs->sources["t1"]->table, $condition);
        $alias = $result->add_source($join, false, false);
        $remap = array("t1" => $alias);
        
        foreach( $rhs->fields as $name => $definition )
        {
          if( !array_key_exists($name, $result->fields) )
          {
            $copy = clone $definition;
            $copy->remap_expressions($result, $remap);
            $result->fields[$name] = $copy;
          }
        }
        
        $join->resolve_expressions($result);
        
        if( !empty($rhs->where_condition) )
        {
          $result->and_where($result->remap_expression($rhs->where_condition, array("t1" => $alias)));
        }
        
      }
      else
      {
        $result->add_source(new SQLQueryJoin($type, $rhs, $condition));
      }
      
      return $result;
    }
    
    
    function left_join( $rhs, $expression )
    {
      return $this->join($rhs, $expression, "left");
    }



    //
    // Adds a calculated field to the query.
    
    function define( $name, $expression )
    {
      $result = clone $this;
      $result->fields[$name] = new SQLQueryDefinedField($this->resolve_expression($expression));
      
      if( preg_match("/(AVG|BIT_AND|BIT_OR|COUNT|GROUP_CONCAT|MAX|MIN|STD|STDDEV_POP|STDDEV_SAMP|STDDEV|SUM|VAR_POP|VAR_SAMP|VARIANCE)\(/i", $expression) )
      {
        $result->has_aggregates = true;
      }
      
      return $result;
    }
    
    
    //
    // Groups the results over the specified fields.
    
    function group_by()
    {
      $result = clone $this;
      $result->group_by_fields = func_get_args();
      return $result;
    }
    
    
    //
    // Orders the results by the specified fields. Pass -1 after any field name to reverse the 
    // sort order of the preceding field.
    
    function order_by()
    {
      $result = clone $this;
      $result->order_by_fields = func_get_args();
      return $result;
    }
    
    
    //
    // Sets a limit on the number of results produced.
    
    function limit( $limit )
    {
      $result = clone $this;
      $result->offset = 0;
      $result->limit  = $limit;
      
      return $result;
    }
    
    
    //
    // Allows you to offset into the results. Returns 1 row, unless you ask for more.
    
    function offset( $offset, $limit = 1 )
    {
      $result = clone $this;
      $result->offset = $offset;
      $result->limit  = $limit;
      
      return $result;
    }
    
    
    
    
    function to_string( $indent = "" )
    {
      ob_start();
      $this->generate_sql($indent);
      return ob_get_clean();
    }
    
    
    function generate_sql( $indent = "" )
    {
      print $indent;
      print "SELECT ";
      
      $first = true;
      foreach( $this->fields as $name => $definition )
      {
        if( $first ) { $first = false; } else { print ", "; }
        $definition->generate_sql($indent);
        if( !is_a($definition, "SQLQueryFieldReference") || $definition->field_name != $name )
        {
          print " as ";
          print $name;
        }
      }
      
      $first = true;
      foreach( $this->sources as $alias => $source )
      {
        print "\n$indent";
        $source->generate_sql($indent, $alias);
      }
      
      if( $this->where_condition )
      {
        print "\n${indent}WHERE $this->where_condition";
      }
      
      if( !empty($this->group_by_fields) )
      {
        print "\n${indent}GROUP BY ";
        $first = true;
        foreach( $this->group_by_fields as $name )
        {
          if( $first ) { $first = false; } else { print ", "; }
          $this->fields[$name]->generate_sql($indent);
        }
      }
      
      if( !empty($this->order_by_fields) )
      {
        print "\n${indent}ORDER BY ";
        $first = true;
        foreach( $this->order_by_fields as $name )
        {
          if( is_numeric($name) )
          {
            if( $name == -1 )
            {
              print " desc";
            }
          }
          else
          {
            if( $first ) { $first = false; } else { print ", "; }
            print "$name";
          }
        }
      }
      
      if( !empty($this->limit) )
      {
        if( !empty($this->offset) )
        {
          printf("\n${indent}LIMIT %d, %d", $this->offset, $this->limit);
        }
        else
        {
          printf("\n${indent}LIMIT %d", $this->limit);
        }
      }
      
    }
    
    
    //
    // Adds a SQLQuery or Table source to the state.
    
    protected function add_source( $source, $import_fields = true, $resolve_expressions = true )
    {
      $source->alias = $alias = $this->next_alias();
      $this->sources[$alias] = $source;
      
      if( $import_fields )
      {
        $this->import_fields($alias);
      }
      
      if( $resolve_expressions )
      {
        $source->resolve_expressions($this);
      }
      
      return $alias;
    }
    
    
    protected function import_fields( $alias )
    {
      foreach( $this->sources[$alias]->field_names() as $name )
      {
        if( !array_key_exists($name, $this->fields) )
        {
          $this->fields[$name] = new SQLQueryFieldReference($alias, $name);
        }
      }
      
      return $alias;
    }
    
    
    //
    // Given an expression full of external names, returns one full of fully-qualified internal
    // names instead. 
    
    function resolve_expression( $expression )
    {
      $resolved = array();
      foreach( SQLExpressionTokenizer::tokenize($expression) as $token )
      {
        if( $token->type == SQLExpressionTokenizer::IDENTIFIER )
        {
          if( array_key_exists($token->string, $this->fields) )
          {
            $resolved[] = $this->fields[$token->string];
          }
          else
          {
            trigger_error("unrecognized field [$token->string] in expression [$expression]", E_USER_ERROR);
          }
        }
        elseif( $token->type == SQLExpressionTokenizer::WORD && array_key_exists($token->string, $this->fields) )
        {
          $resolved[] = $this->fields[$token->string];
        }
        else
        {
          $resolved[] = $token;
        }
      }
      
      return implode("", $resolved);
    }
    
    
    function remap_expression( $expression, $table_alias_mappings )
    {
      $remapped = array();
      foreach( SQLExpressionTokenizer::tokenize($expression) as $token )
      {
        if( $token->type == SQLExpressionTokenizer::DOT )
        {
          $old_alias = array_pop($remapped);
          if( $old_alias->type == SQLExpressionTokenizer::WORD && array_key_exists($old_alias->string, $table_alias_mappings) )
          {
            array_push($remapped, $table_alias_mappings[$old_alias->string]);
          }
          else
          {
            array_push($remapped, $old_alias);
          }
        }
        
        $remapped[] = $token;
      }
      
      return implode("", $remapped);
    }
    
    
    //
    // Returns the next alias for a table.
    
    protected function next_alias()
    {
      $number = count($this->sources) + 1;
      return "t$number";
    }
    
    
    //
    // Appends or sets an already-resolve WHERE expression into the state.
    
    protected function and_where( $expression )
    {
      if( $this->where_condition )
      {
        $this->where_condition = sprintf("(%s) AND (%s)", $this->where_condition, $expression);
      }
      else
      {
        $this->where_condition = $expression;
      }
      
      return $this;
    }
    
    
  }
  
  
  
  class SQLQueryHelper
  {
    function resolve_expressions( $against )
    {
    }
    
    function remap_expressions( $against, $mappings )
    {
    }
    
    function __toString()
    {
      ob_start();
      $this->generate_sql("");
      return ob_get_clean();
    }
  }
  
  
  
  class SQLQuerySource extends SQLQueryHelper
  {
    function __construct( $table, $op = "FROM" )
    {
      if( !is_a($table, "Table") && !is_a($table, "SQLQuery") )
      {
        trigger_error("expected Table of SQLQuery as SQLQuerySource table; got [" . gettype($table) . "]", E_USER_ERROR);
      }
      
      $this->table = $table;
      $this->alias = null;
      $this->op    = $op;
    }
    
    function field_names()
    {
      if( is_a($this->table, "Table") )
      {
        return array_keys($this->table->types);
      }
      elseif( is_a($this->table, "SQLQuery") )
      {
        return array_keys($this->table->fields);
      }
    }
    
    function generate_sql( $indent )
    {
      print "$this->op ";
      
      if( is_a($this->table, "Table") )
      {
        print $this->table->name;
      }
      else
      {
        print "(\n";
        $this->table->generate_sql("$indent  ");
        print "\n$indent)";
      }
      
      if( $this->alias )
      {
        print " ";
        print $this->alias;
      }
    }
    
  }
  
  
  class SQLQueryJoin extends SQLQuerySource
  {
    function __construct( $type, $table, $condition )
    {
      parent::__construct($table, ($type ? strtoupper($type) . " " : "") . "JOIN");
      $this->condition = $condition;
    }
    
    function resolve_expressions( $against )
    {
      $this->condition = $against->resolve_expression($this->condition);
    }
    
    function generate_sql( $indent )
    {
      parent::generate_sql($indent, false);
      if( !empty($this->condition) )
      {
        print " ON ";
        print $this->condition;
      }
    }
  }
  
  
  
  class SQLQueryFieldReference extends SQLQueryHelper
  {
    function __construct( $table_alias, $field_name )
    {
      $this->table_alias = $table_alias;
      $this->field_name  = $field_name;
    }
    
    function generate_sql( $indent )
    {
      printf("%s.%s", $this->table_alias, $this->field_name);
    }
    
    function remap_expressions( $against, $mappings )
    {
      if( array_key_exists($this->table_alias, $mappings) )
      {
        $this->table_alias = $mappings[$this->table_alias];
      }
    }
  }
  
  
  class SQLQueryDefinedField extends SQLQueryHelper
  {
    function __construct( $expression )
    {
      $this->expression = $expression;
    }
    
    function generate_sql( $indent )
    {
      print $this->expression;
    }
    
    function resolve_expressions( $against )
    {
      $this->expression = $against->resolve_expression($this->expression);
    }
    
    function remap_expressions( $against, $mappings )
    {
      $this->expression = $against->remap_expression($this->expression, $mappings);
    }
  }
  
