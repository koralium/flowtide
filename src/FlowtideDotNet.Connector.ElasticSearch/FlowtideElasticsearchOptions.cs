﻿using FlowtideDotNet.Base;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Substrait.Relations;
using Nest;

namespace FlowtideDotNet.Connector.ElasticSearch
{
    public class FlowtideElasticsearchOptions
    {
        public ConnectionSettings? ConnectionSettings { get; set; }

        /// <summary>
        /// Action to apply custom mappings to the index
        /// This will be called on startup.
        /// 
        /// If the index does not exist the properties will be empty.
        /// </summary>
        public Action<IProperties>? CustomMappings { get; set; }

        public Func<WriteRelation, string>? GetIndexNameFunc { get; set; }

        /// <summary>
        /// Function that gets called after the initial data has been saved to elasticsearch.
        /// Parameters are the elasticsearch client, the write relation and the index name.
        /// 
        /// This function can be used for instance to create an alias to the index.
        /// </summary>
        public Func<IElasticClient, WriteRelation, string, Task>? OnInitialDataSent { get; set; }

        /// <summary>
        /// Called each time after data has been sent to elasticsearch
        /// </summary>
        public Func<IElasticClient, WriteRelation, string, Watermark, Task>? OnDataSent { get; set; }

        public ExecutionMode ExecutionMode { get; set; } = ExecutionMode.Hybrid;
    }
}