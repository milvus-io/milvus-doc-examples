package main

import (
	"fmt"

	"github.com/milvus-go-examples/collection"
	"github.com/milvus-go-examples/database"
	"github.com/milvus-go-examples/dml"
	"github.com/milvus-go-examples/rbac"
	"github.com/milvus-go-examples/schema"
	"github.com/milvus-go-examples/search"
	"github.com/milvus-go-examples/util"
)

func userGuideCollection() {
	collection.CreateCollection()
	collection.CreateCollectionWithoutIndex()
	collection.CreateCollectionWithShardNum()
	collection.CreateCollectionWithMmap()
	collection.CreateCollectionWithTTL()
	collection.CreateCollectionWithConsistencyLevel()

	collection.QuickSetup()
	collection.QuickSetupCustomFields()
	collection.ListCollections()
	collection.DescribeCollection()

	collection.RenameCollection()
	collection.SetCollectionProperties()
	collection.DropCollectionProperties()

	collection.SetTTLForCreate()
	collection.SetTTLForExisting()
	collection.DropTTL()

	collection.Load()
	collection.Release()
	collection.LoadFields()

	collection.ListPartitions()
	collection.CreatePartition()
	collection.CheckPartition()
	collection.LoadPartition()
	collection.ReleasePartition()
	collection.DropPartition()

	collection.CreateAlias()
	collection.ListAliases()
	collection.DescribeAlias()
	collection.AlterAlias()
	collection.DropAlias()

	collection.DropCollection()
}

func userGuideSchema() {
	fmt.Println("userGuideSchema()")

	schema.CreateSchema()
	schema.Int64PrimaryKey()
	schema.VarcharParimaryKey()

	schema.DenseVector()
	schema.BinaryVector()
	schema.SparseVector()
	schema.VarcharField()
	schema.NumberField()
	schema.JsonField()
	schema.ArrayField()
	schema.DynamicField()
	schema.NullableField()
	schema.DefaultField()
	schema.AnalyzerOverview()
	schema.AnalyzerBuiltin()
	schema.Tokenizer()
	schema.Filter()
	schema.AlterField()
	schema.SchemaDesign()
	schema.MultiAnalyzer()
}

func userGuideDml() {
	fmt.Println("userGuideDml()")

	dml.Insert()
	dml.Upsert()
	dml.Delete()
}

func userGuideDql() {
	fmt.Println("userGuideDql()")

	util.CreateCollection("my_collection")

	search.BasicSearch()
	search.FilteredSearch()
	search.RangeSearch()
	search.GroupSearch()
	search.Query()

	util.DropCollection("my_collection")

	search.HybridSearch()
	search.FullTextSearch()
	search.TextMatch()
	search.PartitionKey()
	search.Mmap()
	search.ConsistencyLevel()
	search.MetadataFiltering()
}

func userGuideRBAC() {
	rbac.CreateUser()
	rbac.CreateGroup()
	rbac.GrantPrivilige()
	rbac.GrantRole()
	rbac.DropUser()
}

func userGuideDB() {
	database.ManageDatabase()
}

func main() {
	userGuideCollection()
	userGuideSchema()
	userGuideDml()
	userGuideDql()
	userGuideRBAC()
	userGuideDB()
}
