"use client";

import React, { useState, useEffect } from 'react';
import { 
  Activity, 
  Layers, 
  Cpu, 
  Database, 
  Terminal, 
  ShieldCheck, 
  AlertCircle,
  RefreshCw,
  Server,
  Workflow
} from 'lucide-react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  AreaChart,
  Area
} from 'recharts';

export default function EnterpriseDashboard() {
  const [stats, setStats] = useState<any>(null);
  const [tasks, setTasks] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchStats = async () => {
    try {
      const statsRes = await fetch('http://localhost:8080/api/stats');
      if (!statsRes.ok) throw new Error("Gateway error");
      const statsData = await statsRes.json();
      setStats(statsData);

      const tasksRes = await fetch('http://localhost:8080/api/tasks');
      if (!tasksRes.ok) throw new Error("Tasks API error");
      const tasksData = await tasksRes.json();
      setTasks(tasksData);
      
      setError(null);
      setLoading(false);
    } catch (err) {
      console.error("Failed to fetch dashboard data", err);
      setError("Master Node Offline - Attempting Reconnection...");
    }
  };

  useEffect(() => {
    fetchStats();
    const interval = setInterval(fetchStats, 2000);
    return () => clearInterval(interval);
  }, []);

  if (loading || (!stats && !error)) {
    return (
      <div className="min-h-screen bg-[#0a0a0b] flex items-center justify-center text-white font-sans">
        <div className="flex flex-col items-center gap-4">
          <RefreshCw className="animate-spin text-blue-500 w-12 h-12" />
          <p className="text-xl font-light tracking-widest uppercase">Initializing Flowgrid Enterprise</p>
        </div>
      </div>
    );
  }

  if (error && !stats) {
    return (
      <div className="min-h-screen bg-[#0a0a0b] flex items-center justify-center text-white font-sans">
        <div className="flex flex-col items-center gap-4 bg-[#111114] p-8 rounded-3xl border border-red-500/20 shadow-2xl">
          <AlertCircle className="text-red-500 w-16 h-16 animate-pulse" />
          <h2 className="text-2xl font-bold tracking-tight">Cluster Connection Lost</h2>
          <p className="text-gray-500 text-sm max-w-xs text-center leading-relaxed">
            {error}. Ensure the Master node is running and the HTTP Gateway is accessible on port 8080.
          </p>
          <button 
            onClick={fetchStats}
            className="mt-4 px-6 py-2 bg-red-500/10 text-red-500 border border-red-500/20 rounded-xl text-xs font-bold uppercase tracking-widest hover:bg-red-500 hover:text-white transition-all"
          >
            Retry Connection
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-[#0a0a0b] text-white p-6 font-sans">
      {/* Header */}
      <header className="flex justify-between items-center mb-8 bg-[#111114] p-4 rounded-2xl border border-white/5 shadow-2xl">
        <div className="flex items-center gap-3">
          <div className="bg-gradient-to-br from-blue-600 to-indigo-700 p-2 rounded-lg shadow-lg">
            <Workflow className="w-6 h-6 text-white" />
          </div>
          <div>
            <h1 className="text-xl font-bold tracking-tight">Flowgrid <span className="text-blue-500">Enterprise</span></h1>
            <p className="text-[10px] text-gray-500 uppercase tracking-widest">Distributed Container Engine</p>
          </div>
        </div>
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2 bg-green-500/10 px-3 py-1 rounded-full border border-green-500/20">
            <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse" />
            <span className="text-xs font-medium text-green-500 uppercase tracking-wider">System: {stats.system_status}</span>
          </div>
          <div className="flex items-center gap-2 bg-blue-500/10 px-3 py-1 rounded-full border border-blue-500/20">
            <ShieldCheck className="w-3 h-3 text-blue-500" />
            <span className="text-xs font-medium text-blue-500">RBAC Active</span>
          </div>
        </div>
      </header>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        {[
          { label: "Active Workers", value: stats.worker_count, icon: Server, color: "text-blue-500" },
          { label: "Tasks in Queue", value: stats.queue_size, icon: Layers, color: "text-indigo-500" },
          { label: "Avg Latency", value: `${stats.avg_latency}ms`, icon: Activity, color: "text-emerald-500" },
          { label: "Nodes Healthy", value: "100%", icon: Cpu, color: "text-amber-500" },
        ].map((card, i) => (
          <div key={i} className="bg-[#111114] p-5 rounded-2xl border border-white/5 shadow-xl hover:border-blue-500/30 transition-all cursor-default group">
            <div className="flex justify-between items-start mb-2">
              <span className="text-xs text-gray-500 uppercase tracking-widest font-medium">{card.label}</span>
              <card.icon className={`w-5 h-5 ${card.color} group-hover:scale-110 transition-transform`} />
            </div>
            <div className="text-3xl font-bold tracking-tight">{card.value}</div>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Workers List */}
        <div className="lg:col-span-2 bg-[#111114] rounded-2xl border border-white/5 overflow-hidden flex flex-col">
          <div className="p-5 border-b border-white/5 flex justify-between items-center">
            <h2 className="text-sm font-semibold uppercase tracking-widest flex items-center gap-2">
              <Database className="w-4 h-4 text-blue-500" />
              Compute Nodes
            </h2>
          </div>
          <div className="flex-1 overflow-auto p-4">
            <table className="w-full text-left">
              <thead>
                <tr className="text-[10px] text-gray-600 uppercase tracking-widest border-b border-white/5">
                  <th className="pb-3 font-medium">Worker ID</th>
                  <th className="pb-3 font-medium">Status</th>
                  <th className="pb-3 font-medium">CPU</th>
                  <th className="pb-3 font-medium">Memory</th>
                  <th className="pb-3 font-medium">Active Tasks</th>
                </tr>
              </thead>
              <tbody className="text-sm">
                {stats.workers.map((w: any) => (
                  <tr key={w.id} className="border-b border-white/5 hover:bg-white/5 transition-colors group">
                    <td className="py-4 font-mono text-xs text-blue-400">{w.id}</td>
                    <td className="py-4">
                      <span className={`px-2 py-0.5 rounded text-[10px] font-bold uppercase ${
                        w.status === 'IDLE' ? 'bg-blue-500/10 text-blue-500' : 
                        w.status === 'BUSY' ? 'bg-amber-500/10 text-amber-500' : 'bg-red-500/10 text-red-500'
                      }`}>
                        {w.status}
                      </span>
                    </td>
                    <td className="py-4">
                      <div className="w-24 h-1 bg-white/5 rounded-full overflow-hidden">
                        <div className="h-full bg-blue-500" style={{ width: `${w.cpu}%` }} />
                      </div>
                      <span className="text-[10px] text-gray-500 mt-1 block font-mono">{w.cpu}%</span>
                    </td>
                    <td className="py-4">
                      <div className="w-24 h-1 bg-white/5 rounded-full overflow-hidden">
                        <div className="h-full bg-indigo-500" style={{ width: `${w.ram}%` }} />
                      </div>
                      <span className="text-[10px] text-gray-500 mt-1 block font-mono">{w.ram}%</span>
                    </td>
                    <td className="py-4 font-mono text-xs">{w.load}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Task Timeline / DAG Flow */}
        <div className="bg-[#111114] rounded-2xl border border-white/5 flex flex-col">
          <div className="p-5 border-b border-white/5 flex justify-between items-center">
            <h2 className="text-sm font-semibold uppercase tracking-widest flex items-center gap-2">
              <Terminal className="w-4 h-4 text-emerald-500" />
              Live Task Flow
            </h2>
          </div>
          <div className="flex-1 p-4 space-y-4 overflow-auto max-h-[500px]">
            {tasks.length === 0 ? (
              <div className="h-full flex flex-col items-center justify-center text-gray-600 opacity-50">
                <AlertCircle className="w-8 h-8 mb-2" />
                <p className="text-xs font-medium uppercase tracking-widest">No Active Workloads</p>
              </div>
            ) : (
              tasks.map((task) => (
                <div key={task.id} className="bg-black/20 p-3 rounded-xl border border-white/5 flex flex-col gap-2 group hover:border-emerald-500/30 transition-all">
                  <div className="flex justify-between items-start">
                    <span className="text-[10px] font-mono text-gray-500 truncate w-32">{task.id}</span>
                    <span className={`text-[8px] font-bold px-1.5 py-0.5 rounded uppercase ${
                      task.status === 'COMPLETED' ? 'bg-emerald-500/20 text-emerald-500' :
                      task.status === 'RUNNING' ? 'bg-blue-500/20 text-blue-500' : 'bg-gray-500/20 text-gray-500'
                    }`}>
                      {task.status}
                    </span>
                  </div>
                  <div className="flex items-center gap-2">
                    <div className={`p-1.5 rounded-lg ${task.type === 'Docker' ? 'bg-blue-500/10' : 'bg-amber-500/10'}`}>
                      {task.type === 'Docker' ? <Database className="w-3 h-3 text-blue-500" /> : <Layers className="w-3 h-3 text-amber-500" />}
                    </div>
                    <div className="flex flex-col">
                      <span className="text-xs font-semibold">{task.type === 'Docker' ? task.image : 'Python Task'}</span>
                      <span className="text-[9px] text-gray-600 uppercase tracking-widest">{task.type} Runtime</span>
                    </div>
                  </div>
                  {task.depends_on.length > 0 && (
                    <div className="flex items-center gap-1 mt-1 pt-1 border-t border-white/5">
                      <RefreshCw className="w-2 h-2 text-indigo-400" />
                      <span className="text-[8px] text-indigo-400 uppercase tracking-widest">Wait: {task.depends_on.length} deps</span>
                    </div>
                  )}
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
