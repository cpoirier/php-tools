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

  class SQLToken
  {
    function __construct( $type, $string )
    {
      $this->type   = $type;
      $this->string = $string;
    }
  
    function __toString()
    {
      return $this->string;
    }
  }


  class SQLExpressionTokenizer
  {
    const STRING      = 1;
    const IDENTIFIER  = 2;
    const PLACEHOLDER = 3;
    const WORD        = 4;
    const NUMBER      = 5;
    const OPERATOR    = 6;
    const OPEN_PAREN  = 7;
    const CLOSE_PAREN = 8;
    const DOT         = 9;
    const WHITESPACE  = 10;

    function __construct( $expression )
    {
      $this->raw    = $expression;
      $this->pos    = 0;
      $this->tokens = array();
      while( $token = $this->next_token() )
      {
        $this->tokens[] = $token;
      }
    }
    
    static function tokenize( $expression )
    {
      $tokenizer = new static($expression);
      return $tokenizer->tokens;
    }
    
    protected function next_token()
    {
      ob_start();  // We'll print token data to the buffer, rather than do a lot of string concatenation.
      
      $type = null;
      while( !$type && ($c = substr($this->raw, $this->pos++, 1)) )
      {
        print $c;
        switch( $c )
        {
          case "'":
            $type = static::STRING;
            while( $c = substr($this->raw, $this->pos++, 1) )
            {
              print $c;
              if( $c == '\\' )
              {
                print substr($this->raw, $this->pos++, 1);
              }
              elseif( $c == "'" )
              {
                break;
              }
            }
            break;

          case "?":
            $type = static::PLACEHOLDER;
            break;

          case "0":
          case "1": 
          case "2":
          case "3":
          case "4":
          case "5":
          case "6":
          case "7":
          case "8":
          case "9":
            $type = static::NUMBER;
            while( ($c = substr($this->raw, $this->pos, 1)) >= "0" && $c <= "9" )
            {
              print $c;
              $this->pos++;
            }
            break;

          case ' ':
          case '\t':
          case '\r':
          case '\n':
            $type = static::WHITESPACE;
            while( ($c = substr($this->raw, $this->pos, 1)) == ' ' || $c == '\t' || $c == '\r' || $c == '\n' )
            {
              print $c;
              $this->pos++;
            }
            break;

          case ".":
            $type = static::DOT;
            break;
            
          case ',':
          case '(':
          case ')':
          case '+':
          case '-':
          case '*':
          case '/':
          case '=':
            $type = static::OPERATOR;
            break;

          case '<':
            $type = static::OPERATOR;
            if( ($c = substr($this->raw, $this->pos, 1)) == '>' || $c == '=' )
            {
              print $c;
              $this->pos++;
            }
            break;
            
          case '>':
          case '!':
            $type = static::OPERATOR;
            if( ($c = substr($this->raw, $this->pos, 1)) == "=" )
            {
              print $c;
              $this->pos++;
            }
            break;
            
          case '`':
            $type = static::IDENTIFIER;
            ob_clean(); // get rid of the `
            while( ($c = substr($this->raw, $this->pos++, 1)) != '`' )
            {
              print $c;
            }
            break;            

          default:
            $type = static::WORD;
            while( ($c = substr($this->raw, $this->pos, 1)) && preg_match('/[\w\d_]/', $c) )
            {
              print $c;
              $this->pos++;
            }
        }
      }
      
      $string = ob_get_clean();
      
      if( $type == static::WORD )
      {
        $test = strtoupper($string);
        if( $test == "LIKE" || $test == "AS" )
        {
          $type = static::OPERATOR;
        }
        else
        {
          $text = static::IDENTIFIER;
        }        
      }
      return $type ? new SQLToken($type, $string) : null;
    }
    
  }
  
  