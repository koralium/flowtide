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

using Microsoft.EntityFrameworkCore;

namespace FlowtideDotNet.Core.Tests.SmokeTests
{
    public class TpchDbContext : DbContext
    {
        public TpchDbContext(DbContextOptions options) : base(options)
        {
        }

        public DbSet<LineItem> LineItems { get; set; }

        public DbSet<Order> Orders { get; set; }

        public DbSet<Shipmode> Shipmodes { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<LineItem>(b =>
            {
                b.ToTable("lineitems");
                b.HasKey(x => new { x.Orderkey, x.Linenumber });
            });
            modelBuilder.Entity<Order>(b =>
            {
                b.ToTable("orders");
                b.HasKey(x => x.Orderkey);
                b.Property(x => x.Orderkey).ValueGeneratedNever();
            });
            modelBuilder.Entity<Shipmode>(b =>
            {
                b.ToTable("shipmodes");
                b.HasKey(x => x.ShipmodeKey);
                b.Property(x => x.ShipmodeKey).ValueGeneratedNever();
            });
            base.OnModelCreating(modelBuilder);
        }
    }
}
