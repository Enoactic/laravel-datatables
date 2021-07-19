<?php

namespace LaravelDatatables;

use Illuminate\Support\Facades\DB;

class DatatablesMSSQL
{
    public $connection = "sqlsrv";

    public function raw($request, $table, $filters, $joins = [], $defaultWhere = null, $defaultGroup = null, $debug = false)
    {
        return $this->getResults($request, $table, $filters, $joins, $defaultWhere, $defaultGroup, $debug);
    }

    public function get($request, $table, $filters, $joins = [], $defaultWhere = null, $defaultGroup = null, $debug = false)
    {
        $output = array(
            "draw" => $request->input('draw'),
            "data" => $this->getResults($request, $table, $filters, $joins, $defaultWhere, $defaultGroup, $debug),
            "recordsFiltered" => $this->getResultCount($request, $table, $filters, $joins, $defaultWhere, $defaultGroup, $debug),
            "recordsTotal" => $this->getTotalCount($request, $table, $filters, $joins, $defaultWhere, $defaultGroup, $debug),
        );

        return $output;
    }

    public function getResults($request, $table, $filters, $joins, $defaultWhere, $defaultGroup, $debug)
    {
        // SELECT
        $selectStatement = "";
        foreach($filters as $filterKey => $filterValue)
        {
            if($filterValue["type"] == "FULL_DATE")
            {
                if($selectStatement != "")
                    $selectStatement = $selectStatement . ", CONVERT(VARCHAR(20), " . $filterKey . ", 126) AS '" . $filterValue["alias"] . "'";
                else
                    $selectStatement = $filterKey . " AS '" . $filterValue["alias"] . "'";
            }
            else
            {
                if($selectStatement != "")
                    $selectStatement = $selectStatement . ", " . $filterKey . " AS '" . $filterValue["alias"] . "'";
                else
                    $selectStatement = $filterKey . " AS '" . $filterValue["alias"] . "'";
            }
        }
        // JOIN
        $joinStatement = "";
        foreach($joins as $joinKey => $joinValue)
        {
            if($joinStatement != "")
                $joinStatement = $joinStatement . " LEFT JOIN " . $joinKey . " ON " . $joinValue;
            else
                $joinStatement = " LEFT JOIN " . $joinKey . " ON " . $joinValue;
        }
        // WHERE
        $whereStatement = $defaultWhere;
        foreach($filters as $filterKey => $filterValue)
        {
            if($filterValue["value"] != "")
            {
                $field = $filterKey;
                $value = $filterValue["value"];
                $method = $filterValue["method"];
                if($method == "LIKE"){
                    $whereStatement .= ($whereStatement == "") ? " " . $field . " LIKE '%" . $value . "%'" : " AND " . $field . " LIKE '%" . $value . "%'";
                }else if($method == "EQUAL"){
                    $whereStatement .= ($whereStatement == "") ? " " . $field . " = '" . $value . "%" : " AND " . $field . " = '" . $value . "'";
                }else if($method == "DATE"){
                    $date = null;
                    $dateFormats = array("d-F-Y", "d F Y", "Y m d", "Y-m-d");
                    foreach($dateFormats as $dateFormat){
                        if($date == null && \DateTime::createFromFormat($dateFormat, $value) !== FALSE){
                            $date = date("Y-m-d", strtotime($value));
                        }
                    }
                    if($date != null){
                        $whereStatement .= ($whereStatement == "") ? " CONVERT(VARCHAR(20), " . $field . ", 126) = '" . $value . "'" : " AND CONVERT(VARCHAR(20), " . $field . ", 126) = '" . $value . "'";
                    }
                }else if($method == "DATE_BETWEEN"){
                    $dateStart = null;
                    $dateEnd = null;
                    $dateFormats = array("d-F-Y", "d F Y", "Y m d", "Y-m-d");

                    $value = explode(" AND ", $value);
                    $valueStart = $value[0] ?? null;
                    $valueEnd = $value[1] ?? null;
                    if($valueStart != null){
                        foreach($dateFormats as $dateFormat){
                            if($dateStart == null && \DateTime::createFromFormat($dateFormat, $valueStart) !== FALSE){
                                $dateStart = date("Y-m-d", strtotime($valueStart));
                            }
                        }
                    }
                    if($valueEnd != null){
                        foreach($dateFormats as $dateFormat){
                            if($dateEnd == null && \DateTime::createFromFormat($dateFormat, $valueEnd) !== FALSE){
                                $dateEnd = date("Y-m-d", strtotime($valueEnd));
                            }
                        }
                    }
                    if($dateStart != null && $dateEnd != null){
                        $whereStatement .= ($whereStatement == "") ? " CONVERT(VARCHAR(20), " . $field . ", 126) BETWEEN '" . $dateStart . "' AND '" . $dateEnd . "'" : " AND CONVERT(VARCHAR(20), " . $field . ", 126) BETWEEN '" . $dateStart . "' AND '" . $dateEnd . "'";
                    }else if($dateStart != null){
                        $whereStatement .= ($whereStatement == "") ? "CONVERT(VARCHAR(20), " . $field . ", 126) = '" . $dateStart . "'" : " AND CONVERT(VARCHAR(20), " . $field . ", 126) = '" . $dateStart . "'";
                    }
                }
            }
        }
        $filterStatement = "";
        if($request->input('search.value') != "")
        {
            foreach($filters as $filterKey => $filterValue)
            {
                $field = $filterKey;
                $value = $request->input('search.value');
                $method = $filterValue["method"];
                if($method == "LIKE"){
                    $filterStatement .= ($filterStatement == "") ? " " . $field . " LIKE '%" . $value . "%'" : " AND " . $field . " LIKE '%" . $value . "%'";
                }else if($method == "EQUAL"){
                    $filterStatement .= ($filterStatement == "") ? " " . $field . " = '" . $value . "%" : " AND " . $field . " = '" . $value . "'";
                }else if($method == "DATE"){
                    $date = null;
                    $dateFormats = array("d-F-Y", "d F Y", "Y m d", "Y-m-d");
                    foreach($dateFormats as $dateFormat){
                        if($date == null && \DateTime::createFromFormat($dateFormat, $value) !== FALSE){
                            $date = date("Y-m-d", strtotime($value));
                        }
                    }
                    if($date != null){
                        $filterStatement .= ($filterStatement == "") ? " CONVERT(VARCHAR(20), " . $field . ", 126) = '" . $value . "'" : " AND CONVERT(VARCHAR(20), " . $field . ", 126) = '" . $value . "'";
                    }
                }else if($method == "DATE_BETWEEN"){
                    $dateStart = null;
                    $dateEnd = null;
                    $dateFormats = array("d-F-Y", "d F Y", "Y m d", "Y-m-d");

                    $value = explode(" AND ", $value);
                    $valueStart = $value[0] ?? null;
                    $valueEnd = $value[1] ?? null;
                    if($valueStart != null){
                        foreach($dateFormats as $dateFormat){
                            if($dateStart == null && \DateTime::createFromFormat($dateFormat, $valueStart) !== FALSE){
                                $dateStart = date("Y-m-d", strtotime($valueStart));
                            }
                        }
                    }
                    if($valueEnd != null){
                        foreach($dateFormats as $dateFormat){
                            if($dateEnd == null && \DateTime::createFromFormat($dateFormat, $valueEnd) !== FALSE){
                                $dateEnd = date("Y-m-d", strtotime($valueEnd));
                            }
                        }
                    }
                    if($dateStart != null && $dateEnd != null){
                        $filterStatement .= ($filterStatement == "") ? " CONVERT(VARCHAR(20), " . $field . ", 126) BETWEEN '" . $dateStart . "' AND '" . $dateEnd . "'" : " AND CONVERT(VARCHAR(20), " . $field . ", 126) BETWEEN '" . $dateStart . "' AND '" . $dateEnd . "'";
                    }else if($dateStart != null){
                        $filterStatement .= ($filterStatement == "") ? "CONVERT(VARCHAR(20), " . $field . ", 126) = '" . $dateStart . "'" : " AND CONVERT(VARCHAR(20), " . $field . ", 126) = '" . $dateStart . "'";
                    }
                }
            }
        }
        if($whereStatement != "" && $filterStatement != "")
            $whereStatement = " WHERE (".$whereStatement.") AND (".$filterStatement.")";
        else if($whereStatement != "")
            $whereStatement = " WHERE (".$whereStatement.")";
        else if($filterStatement != "")
            $whereStatement = " WHERE (".$filterStatement.")";
        // GROUP
        $groupStatement = "";
        if($defaultGroup != "") $groupStatement = " GROUP BY ".$defaultGroup;
        // ORDER
        $orderStatement = "";
        foreach($filters as $filterKey => $filterValue)
        {
            if($filterValue["alias"] == $request->input('columns.'.$request->input('order.0.column').'.data'))
                $orderStatement = $filterKey." ".$request->input('order.0.dir');
        }
        if($orderStatement != "") $orderStatement = " ORDER BY ".$orderStatement;
        // LIMIT
        $limit = "";
        if($request->input('start') != "" && $request->input('length') != ""){
            $limit = " WHERE rownumber >= ".$request->input('start')." AND rownumber < ". ($request->input('start') + $request->input('length'));
        }

        $query = "WITH RESULTS AS (SELECT ROW_NUMBER() OVER(" . $orderStatement . ") AS rownumber, " . $selectStatement . " FROM " . $table . $joinStatement . $whereStatement . $groupStatement . ") SELECT * FROM RESULTS".$limit;

        if($debug){
            return $query;
        }else{
            return DB::connection($this->connection)->select($query);
        }
    }

    public function getResultCount($request, $table, $filters, $joins, $defaultWhere, $defaultGroup, $debug)
    {
        // JOIN
        $joinStatement = "";
        foreach($joins as $joinKey => $joinValue)
        {
            if($joinStatement != "")
                $joinStatement = $joinStatement . " LEFT JOIN " . $joinKey . " ON " . $joinValue;
            else
                $joinStatement = " LEFT JOIN " . $joinKey . " ON " . $joinValue;
        }
        // WHERE
        $whereStatement = $defaultWhere;
        foreach($filters as $filterKey => $filterValue)
        {
            if($filterValue["value"] != "")
            {
                $field = $filterKey;
                $value = $filterValue["value"];
                $method = $filterValue["method"];
                if($method == "LIKE"){
                    $whereStatement .= ($whereStatement == "") ? " " . $field . " LIKE '%" . $value . "%'" : " AND " . $field . " LIKE '%" . $value . "%'";
                }else if($method == "EQUAL"){
                    $whereStatement .= ($whereStatement == "") ? " " . $field . " = '" . $value . "%" : " AND " . $field . " = '" . $value . "'";
                }else if($method == "DATE"){
                    $date = null;
                    $dateFormats = array("d-F-Y", "d F Y", "Y m d", "Y-m-d");
                    foreach($dateFormats as $dateFormat){
                        if($date == null && \DateTime::createFromFormat($dateFormat, $value) !== FALSE){
                            $date = date("Y-m-d", strtotime($value));
                        }
                    }
                    if($date != null){
                        $whereStatement .= ($whereStatement == "") ? " CONVERT(VARCHAR(20), " . $field . ", 126) = '" . $value . "'" : " AND CONVERT(VARCHAR(20), " . $field . ", 126) = '" . $value . "'";
                    }
                }else if($method == "DATE_BETWEEN"){
                    $dateStart = null;
                    $dateEnd = null;
                    $dateFormats = array("d-F-Y", "d F Y", "Y m d", "Y-m-d");

                    $value = explode(" AND ", $value);
                    $valueStart = $value[0] ?? null;
                    $valueEnd = $value[1] ?? null;
                    if($valueStart != null){
                        foreach($dateFormats as $dateFormat){
                            if($dateStart == null && \DateTime::createFromFormat($dateFormat, $valueStart) !== FALSE){
                                $dateStart = date("Y-m-d", strtotime($valueStart));
                            }
                        }
                    }
                    if($valueEnd != null){
                        foreach($dateFormats as $dateFormat){
                            if($dateEnd == null && \DateTime::createFromFormat($dateFormat, $valueEnd) !== FALSE){
                                $dateEnd = date("Y-m-d", strtotime($valueEnd));
                            }
                        }
                    }
                    if($dateStart != null && $dateEnd != null){
                        $whereStatement .= ($whereStatement == "") ? " CONVERT(VARCHAR(20), " . $field . ", 126) BETWEEN '" . $dateStart . "' AND '" . $dateEnd . "'" : " AND CONVERT(VARCHAR(20), " . $field . ", 126) BETWEEN '" . $dateStart . "' AND '" . $dateEnd . "'";
                    }else if($dateStart != null){
                        $whereStatement .= ($whereStatement == "") ? "CONVERT(VARCHAR(20), " . $field . ", 126) = '" . $dateStart . "'" : " AND CONVERT(VARCHAR(20), " . $field . ", 126) = '" . $dateStart . "'";
                    }
                }
            }
        }
        $filterStatement = "";
        if($request->input('search.value') != "")
        {
            foreach($filters as $filterKey => $filterValue)
            {
                $field = $filterKey;
                $value = $request->input('search.value');
                $method = $filterValue["method"];
                if($method == "LIKE"){
                    $filterStatement .= ($filterStatement == "") ? " " . $field . " LIKE '%" . $value . "%'" : " AND " . $field . " LIKE '%" . $value . "%'";
                }else if($method == "EQUAL"){
                    $filterStatement .= ($filterStatement == "") ? " " . $field . " = '" . $value . "%" : " AND " . $field . " = '" . $value . "'";
                }else if($method == "DATE"){
                    $date = null;
                    $dateFormats = array("d-F-Y", "d F Y", "Y m d", "Y-m-d");
                    foreach($dateFormats as $dateFormat){
                        if($date == null && \DateTime::createFromFormat($dateFormat, $value) !== FALSE){
                            $date = date("Y-m-d", strtotime($value));
                        }
                    }
                    if($date != null){
                        $filterStatement .= ($filterStatement == "") ? " CONVERT(VARCHAR(20), " . $field . ", 126) = '" . $value . "'" : " AND CONVERT(VARCHAR(20), " . $field . ", 126) = '" . $value . "'";
                    }
                }else if($method == "DATE_BETWEEN"){
                    $dateStart = null;
                    $dateEnd = null;
                    $dateFormats = array("d-F-Y", "d F Y", "Y m d", "Y-m-d");

                    $value = explode(" AND ", $value);
                    $valueStart = $value[0] ?? null;
                    $valueEnd = $value[1] ?? null;
                    if($valueStart != null){
                        foreach($dateFormats as $dateFormat){
                            if($dateStart == null && \DateTime::createFromFormat($dateFormat, $valueStart) !== FALSE){
                                $dateStart = date("Y-m-d", strtotime($valueStart));
                            }
                        }
                    }
                    if($valueEnd != null){
                        foreach($dateFormats as $dateFormat){
                            if($dateEnd == null && \DateTime::createFromFormat($dateFormat, $valueEnd) !== FALSE){
                                $dateEnd = date("Y-m-d", strtotime($valueEnd));
                            }
                        }
                    }
                    if($dateStart != null && $dateEnd != null){
                        $filterStatement .= ($filterStatement == "") ? " CONVERT(VARCHAR(20), " . $field . ", 126) BETWEEN '" . $dateStart . "' AND '" . $dateEnd . "'" : " AND CONVERT(VARCHAR(20), " . $field . ", 126) BETWEEN '" . $dateStart . "' AND '" . $dateEnd . "'";
                    }else if($dateStart != null){
                        $filterStatement .= ($filterStatement == "") ? "CONVERT(VARCHAR(20), " . $field . ", 126) = '" . $dateStart . "'" : " AND CONVERT(VARCHAR(20), " . $field . ", 126) = '" . $dateStart . "'";
                    }
                }
            }
        }
        if($whereStatement != "" && $filterStatement != "")
            $whereStatement = " WHERE (".$whereStatement.") AND (".$filterStatement.")";
        else if($whereStatement != "")
            $whereStatement = " WHERE (".$whereStatement.")";
        else if($filterStatement != "")
            $whereStatement = " WHERE (".$filterStatement.")";
        // GROUP
        $groupStatement = "";
        if($defaultGroup != "") $groupStatement = " GROUP BY ".$defaultGroup;

        $query = "SELECT
                        COUNT(*) AS 'total'
                    FROM $table
                        $joinStatement
                    $whereStatement $groupStatement";

        if($debug){
            return $query;
        }else{
            $results = DB::connection($this->connection)->select($query);
            return $results[0]->total;
        }
    }

    public function getTotalCount($request, $table, $filters, $joins, $defaultWhere, $defaultGroup, $debug)
    {
        // JOIN
        $joinStatement = "";
        foreach($joins as $joinKey => $joinValue)
        {
            if($joinStatement != "")
                $joinStatement = $joinStatement . " LEFT JOIN " . $joinKey . " ON " . $joinValue;
            else
                $joinStatement = " LEFT JOIN " . $joinKey . " ON " . $joinValue;
        }
        // WHERE
        $whereStatement = $defaultWhere;
        if($whereStatement != "") $whereStatement = " WHERE ".$whereStatement;
        // GROUP
        $groupStatement = "";
        if($defaultGroup != "") $groupStatement = " GROUP BY ".$defaultGroup;

        $query = "SELECT
                        COUNT(*) AS 'total'
                    FROM $table
                        $joinStatement
                    $whereStatement $groupStatement";

        if($debug){
            return $query;
        }else{
            $results = DB::connection($this->connection)->select($query);
            return $results[0]->total;
        }
    }
}
