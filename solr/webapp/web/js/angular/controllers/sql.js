/*
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

solrAdminApp.controller('SQLController',
  function($scope, $routeParams, $location, SQL, Constants){
    $scope.resetMenu("sql", Constants.IS_COLLECTION_PAGE);

    $scope.sql = {wt: 'json', indent:'on'};

    if ($location.search().stmt) {
      $scope.sql.stmt = $location.search()["stmt"];
    }

    $scope.doQuery = function() {
      var params = {};

      var set = function(key, value) {
        if (params[key]) {
          params[key].push(value);
        } else {
          params[key] = [value];
        }
      }
      var copy = function(params, query) {
        for (var key in query) {
          terms = query[key];
          if (terms.length > 0 && key[0]!="$") {
            set(key, terms);
          }
        }
      };

      copy(params, $scope.sql);

      if ($scope.rawParams) {
        var rawParams = $scope.rawParams.split(/[&\n]/);
        for (var i in rawParams) {
            var param = rawParams[i];
            var parts = param.split("=");
            set(parts[0], parts[1]);
        }
      }

      params.core = $routeParams.core;
      var url = SQL.url(params);
      SQL.query(params, function(data) {
        $scope.lang = $scope.sql.wt;
        $scope.response = data;
        $scope.url = $location.protocol() + "://" +
                     $location.host() + ":" +
                     $location.port() + url;
      });
    };
  }
);
