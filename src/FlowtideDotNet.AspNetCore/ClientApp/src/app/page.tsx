"use client"
import { CacheHitsChart } from "./components/cachehitschart";
import { Card } from "./components/card";
import { DiskCacheWriteChart } from "./components/diskcachewritechart";
import { EventsProcessedChart } from "./components/eventsprocessedchart";
import Header from "./components/header";
import { MemoryUsageChart } from "./components/memoryusagechart";
import { OperatorTable } from "./components/operatortable";
import { PageCountChart } from "./components/pagecountchart";
import { StreamDisplay } from "./components/streamgraph";

export default function Home() {

  return (
    <div className="App">
       <Header />
       <div className="grid grid-cols-12 gap-1">
        <div className="col-start-2 col-span-10">
          <Card>
            <EventsProcessedChart />
          </Card>
        </div>
        <div className="col-start-2 col-span-10">
          <Card>
            <StreamDisplay />
          </Card>
        </div>
        <div className="col-start-2 col-span-5">
          <Card>
            <MemoryUsageChart />
          </Card>
        </div>
        <div className="col-start-7 col-span-5">
          <Card>
            <PageCountChart />
          </Card>
        </div>
        <div className="col-start-2 col-span-5">
          <Card>
            <DiskCacheWriteChart />
          </Card>
        </div>
        <div className="col-start-7 col-span-5">
          <Card>
            <CacheHitsChart />
          </Card>
        </div>
        <div className="col-start-2 col-span-10">
          <Card>
            <OperatorTable />
          </Card>
        </div>
      </div>
    </div>
  );
}
