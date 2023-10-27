($CreateCommand = Get-Content -Path './tables.sql' -Raw) | out-null;
($SelectCommand = Get-Content -Path './SelectLineItems/query.sql' -Raw) | out-null;
java -jar isthmus-0.15.0-all.jar -c $CreateCommand $SelectCommand > ./SelectLineItems/queryplan.json

($SelectCommand = Get-Content -Path './FilterLineItemsOnShipmode/query.sql' -Raw) | out-null;
java -jar isthmus-0.15.0-all.jar -c $CreateCommand $SelectCommand > ./FilterLineItemsOnShipmode/queryplan.json

($SelectCommand = Get-Content -Path './LineItemLeftJoinOrders/query.sql' -Raw) | out-null;
java -jar isthmus-0.15.0-all.jar -c $CreateCommand $SelectCommand > ./LineItemLeftJoinOrders/queryplan.json

($SelectCommand = Get-Content -Path './StringJoin/query.sql' -Raw) | out-null;
java -jar isthmus-0.15.0-all.jar -c $CreateCommand $SelectCommand > ./StringJoin/queryplan.json

($SelectCommand = Get-Content -Path './LeftJoinUpdateLeftValues/query.sql' -Raw) | out-null;
java -jar isthmus-0.15.0-all.jar -c $CreateCommand $SelectCommand > ./LeftJoinUpdateLeftValues/queryplan.json

($SelectCommand = Get-Content -Path './Count/query.sql' -Raw) | out-null;
java -jar isthmus-0.15.0-all.jar -c $CreateCommand $SelectCommand > ./Count/queryplan.json