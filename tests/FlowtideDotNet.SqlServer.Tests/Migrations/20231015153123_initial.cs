// Licensed under the Apache License, Version 2.0 (the "License")
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

using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace FlowtideDotNet.SqlServer.Tests.Migrations
{
    /// <inheritdoc />
    public partial class initial : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "lineitems",
                columns: table => new
                {
                    Orderkey = table.Column<long>(type: "bigint", nullable: false),
                    Linenumber = table.Column<int>(type: "int", nullable: false),
                    Partkey = table.Column<long>(type: "bigint", nullable: false),
                    Suppkey = table.Column<long>(type: "bigint", nullable: false),
                    Quantity = table.Column<double>(type: "float", nullable: false),
                    Extendedprice = table.Column<double>(type: "float", nullable: false),
                    Discount = table.Column<double>(type: "float", nullable: false),
                    Tax = table.Column<double>(type: "float", nullable: false),
                    Returnflag = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    Linestatus = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    Shipdate = table.Column<DateTime>(type: "datetime2", nullable: false),
                    Commitdate = table.Column<DateTime>(type: "datetime2", nullable: false),
                    Receiptdate = table.Column<DateTime>(type: "datetime2", nullable: false),
                    Shipinstruct = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    Shipmode = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    Comment = table.Column<string>(type: "nvarchar(max)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_lineitems", x => new { x.Orderkey, x.Linenumber });
                });

            migrationBuilder.CreateTable(
                name: "orders",
                columns: table => new
                {
                    Orderkey = table.Column<long>(type: "bigint", nullable: false),
                    Custkey = table.Column<long>(type: "bigint", nullable: false),
                    Orderstatus = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    Totalprice = table.Column<double>(type: "float", nullable: false),
                    Orderdate = table.Column<DateTime>(type: "datetime2", nullable: false),
                    Orderpriority = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    Clerk = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    Shippriority = table.Column<int>(type: "int", nullable: false),
                    Comment = table.Column<string>(type: "nvarchar(max)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_orders", x => x.Orderkey);
                });

            migrationBuilder.CreateTable(
                name: "shipmodes",
                columns: table => new
                {
                    ShipmodeKey = table.Column<int>(type: "int", nullable: false),
                    Mode = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    Cost = table.Column<double>(type: "float", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_shipmodes", x => x.ShipmodeKey);
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "lineitems");

            migrationBuilder.DropTable(
                name: "orders");

            migrationBuilder.DropTable(
                name: "shipmodes");
        }
    }
}
