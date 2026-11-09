import React, { useState, useEffect, useRef } from 'react';
import {
  Activity,
  Terminal,
  Table as TableIcon,
  CreditCard,
  AlertTriangle,
  TrendingUp,
  RefreshCw,
  Play,
  Users,
  Building2,
  Clock,
  Layers,
  Network,
  ChevronDown,
  ChevronRight,
  Code,
  Sparkles
} from 'lucide-react';
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid
} from 'recharts';
import { DEFAULT_SCHEMA_METADATA } from './schemaData';
import './App.css';

const API_BASE = 'http://127.0.0.1:8000/api';

const SQL_PRESETS = {
  "1. Doanh số & Tỷ lệ gian lận theo Quý": `SELECT 
    year,
    quarter,
    COUNT(DISTINCT merchant_sk) AS active_merchants,
    SUM(total_txn) AS quarterly_txns,
    ROUND(SUM(total_volume), 2) AS quarterly_volume,
    SUM(fraud_count) AS quarterly_frauds,
    ROUND(SUM(total_volume) / SUM(total_txn), 2) AS avg_txn_amount,
    ROUND(SUM(fraud_count) * 100.0 / SUM(total_txn), 4) AS fraud_rate_pct
FROM agg_monthly_merchant_vol
GROUP BY year, quarter
ORDER BY year DESC, quarter DESC;`,

  "2. Phân tích rủi ro theo hình thức quẹt thẻ": `SELECT 
    f.use_chip,
    COUNT(*) AS total_transactions,
    ROUND(SUM(f.amount), 2) AS total_volume,
    SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS fraud_rate_pct,
    ROUND(AVG(f.amount), 2) AS avg_amount
FROM fact_transactions f
GROUP BY f.use_chip
ORDER BY fraud_rate_pct DESC;`,

  "3. Kiểm chứng lịch sử thẻ (SCD Type 2 Point-in-Time)": `SELECT 
    f.card_sk,
    c.card_index,
    c.credit_limit,
    c.card_on_dark_web,
    c.effective_date,
    c.expiration_date,
    c.is_current,
    COUNT(*) AS so_giao_dich,
    ROUND(SUM(f.amount), 2) AS tong_tien,
    MIN(f.transaction_timestamp) AS giao_dich_som_nhat,
    MAX(f.transaction_timestamp) AS giao_dich_muon_nhat
FROM fact_transactions f
JOIN dim_cards c ON f.card_sk = c.card_sk
WHERE c.user_id = 0 AND c.card_index = 0
GROUP BY f.card_sk, c.card_index, c.credit_limit, c.card_on_dark_web, c.effective_date, c.expiration_date, c.is_current
ORDER BY f.card_sk;`,

  "4. Top 10 đối tác điểm bán có doanh thu cao nhất": `SELECT 
    m.merchant_name,
    m.merchant_city,
    m.merchant_state,
    COUNT(*) AS total_txns,
    ROUND(SUM(f.amount), 2) AS total_volume,
    SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS fraud_rate_pct
FROM fact_transactions f
JOIN dim_merchant m ON f.merchant_sk = m.merchant_sk
GROUP BY m.merchant_name, m.merchant_city, m.merchant_state
ORDER BY total_volume DESC
LIMIT 10;`,

  "5. Phân nhóm chi tiêu theo độ tuổi khách hàng": `SELECT 
    d.year,
    CASE 
        WHEN u.current_age < 30 THEN 'Dưới 30 tuổi'
        WHEN u.current_age BETWEEN 30 AND 50 THEN '30 - 50 tuổi'
        ELSE 'Trên 50 tuổi'
    END AS age_group,
    COUNT(*) AS total_txns,
    ROUND(SUM(f.amount), 2) AS total_volume,
    SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS fraud_count
FROM fact_transactions f
JOIN dim_users u ON f.user_sk = u.user_sk
JOIN dim_date d ON f.date_sk = d.date_sk
GROUP BY d.year, age_group
ORDER BY d.year DESC, age_group;`
};

const SQL_KEYWORDS = [
  'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT JOIN', 'INNER JOIN', 'ON',
  'GROUP BY', 'ORDER BY', 'DESC', 'ASC', 'LIMIT', 'COUNT', 'SUM',
  'AVG', 'ROUND', 'DISTINCT', 'AS', 'AND', 'OR', 'NOT', 'IN', 'IS',
  'NULL', 'LIKE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'HAVING'
];

export default function App() {
  const [activeTab, setActiveTab] = useState('streaming');
  const [isLivePolling, setIsLivePolling] = useState(false);

  // Streaming Data States
  const [systemTime, setSystemTime] = useState(() => new Date().toLocaleTimeString());
  const [streamingKpi, setStreamingKpi] = useState(null);
  const [liveFeed, setLiveFeed] = useState({ recent_transactions: [], timeline: [], chip_distribution: [] });

  // Gold DWH States
  const [goldKpi, setGoldKpi] = useState(null);
  const [monthlyTrends, setMonthlyTrends] = useState([]);
  const [quarterlyTrends, setQuarterlyTrends] = useState([]);
  const [cardFraudRisks, setCardFraudRisks] = useState([]);

  // Schema Metadata & ERD States (Khởi tạo sẵn từ schemaData để không bao giờ bị trang trắng)
  const [schemaMetadata, setSchemaMetadata] = useState(DEFAULT_SCHEMA_METADATA);
  const [erdFilter, setErdFilter] = useState('all'); // 'all', 'core', 'summary'

  // SQL Studio States
  const [selectedPreset, setSelectedPreset] = useState(Object.keys(SQL_PRESETS)[0]);
  const [sqlQuery, setSqlQuery] = useState(SQL_PRESETS[Object.keys(SQL_PRESETS)[0]]);
  const [sqlResult, setSqlResult] = useState(null);
  const [sqlError, setSqlError] = useState(null);
  const [sqlRunning, setSqlRunning] = useState(false);
  const [schemaSearch, setSchemaSearch] = useState('');
  const [expandedTables, setExpandedTables] = useState({ fact_transactions: true, dim_cards: true });

  // SQL Inline Autocomplete States
  const [suggestions, setSuggestions] = useState([]);
  const [activeSuggestionIdx, setActiveSuggestionIdx] = useState(0);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const textareaRef = useRef(null);

  // Data Inspector States
  const [tableList, setTableList] = useState([]);
  const [selectedTable, setSelectedTable] = useState('fact_transactions');
  const [tableData, setTableData] = useState(null);
  const [inspectorLoading, setInspectorLoading] = useState(false);

  // Danh mục từ điển cho tính năng tự động gợi ý (Autocomplete Dictionary)
  const autocompleteDict = React.useMemo(() => {
    const list = [];
    // Keywords
    SQL_KEYWORDS.forEach(kw => list.push({ text: kw, kind: 'keyword' }));
    // Tables
    schemaMetadata.forEach(tbl => {
      list.push({ text: tbl.table_name, kind: 'table', desc: tbl.description });
      // Columns
      tbl.columns.forEach(col => {
        list.push({
          text: col.name,
          kind: 'column',
          parentTable: tbl.table_name,
          dataType: col.type,
          desc: col.desc || ''
        });
      });
    });
    return list;
  }, [schemaMetadata]);

  // Fetch initial data
  const fetchStreamingData = async () => {
    try {
      const [resKpi, resFeed] = await Promise.all([
        fetch(`${API_BASE}/kpi/streaming`),
        fetch(`${API_BASE}/streaming/live-feed`)
      ]);
      if (resKpi.ok) setStreamingKpi(await resKpi.json());
      if (resFeed.ok) setLiveFeed(await resFeed.json());
    } catch (e) {
      console.warn("API streaming call failed (server starting...):", e.message);
    }
  };

  const fetchGoldData = async () => {
    try {
      const [resKpi, resM, resQ, resF] = await Promise.all([
        fetch(`${API_BASE}/kpi/gold`),
        fetch(`${API_BASE}/gold/monthly-trends`),
        fetch(`${API_BASE}/gold/quarterly-trends`),
        fetch(`${API_BASE}/gold/card-fraud-risks`)
      ]);
      if (resKpi.ok) setGoldKpi(await resKpi.json());
      if (resM.ok) setMonthlyTrends(await resM.json());
      if (resQ.ok) setQuarterlyTrends(await resQ.json());
      if (resF.ok) setCardFraudRisks(await resF.json());
    } catch (e) {
      console.warn("API gold call failed:", e.message);
    }
  };

  const fetchSchemaMetadata = async () => {
    try {
      const res = await fetch(`${API_BASE}/schema/metadata`);
      if (res.ok) {
        const data = await res.json();
        if (data && data.length > 0) setSchemaMetadata(data);
      }
    } catch (e) {
      console.warn("API schema call failed, fallback used:", e.message);
    }
  };

  const fetchTableList = async () => {
    try {
      const res = await fetch(`${API_BASE}/inspector/tables`);
      if (res.ok) setTableList(await res.json());
    } catch (e) {
      console.warn("API table list failed:", e.message);
    }
  };

  const fetchTableDetail = async (tbl) => {
    setInspectorLoading(true);
    try {
      const res = await fetch(`${API_BASE}/inspector/table/${tbl}?limit=25`);
      if (res.ok) setTableData(await res.json());
    } catch (e) {
      console.warn("API table detail failed:", e.message);
    } finally {
      setInspectorLoading(false);
    }
  };

  useEffect(() => {
    fetchStreamingData();
    fetchGoldData();
    fetchSchemaMetadata();
    fetchTableList();

    const clockTimer = setInterval(() => {
      setSystemTime(new Date().toLocaleTimeString());
    }, 1000);
    return () => clearInterval(clockTimer);
  }, []);

  // Poll for streaming live feed
  useEffect(() => {
    if (!isLivePolling || activeTab !== 'streaming') return;
    const interval = setInterval(() => {
      fetchStreamingData();
    }, 3000);
    return () => clearInterval(interval);
  }, [isLivePolling, activeTab]);

  // When changing table in Inspector
  useEffect(() => {
    if (activeTab === 'inspector') {
      fetchTableDetail(selectedTable);
    }
  }, [activeTab, selectedTable]);

  // Handle SQL text change & trigger inline Autocomplete
  const handleSqlChange = (e) => {
    const text = e.target.value;
    const cursor = e.target.selectionStart;
    setSqlQuery(text);

    // Tìm từ hiện tại mà người dùng đang gõ
    const textBeforeCursor = text.slice(0, cursor);
    const match = textBeforeCursor.match(/([a-zA-Z0-9_]+)$/);

    if (match && match[1].length >= 1) {
      const currentWord = match[1].toLowerCase();
      // Lọc danh sách gợi ý
      const filtered = autocompleteDict.filter(item =>
        item.text.toLowerCase().startsWith(currentWord) ||
        (item.desc && item.desc.toLowerCase().includes(currentWord))
      ).slice(0, 8); // Top 8 gợi ý

      if (filtered.length > 0) {
        setSuggestions(filtered);
        setActiveSuggestionIdx(0);
        setShowSuggestions(true);
      } else {
        setShowSuggestions(false);
      }
    } else {
      setShowSuggestions(false);
    }
  };

  // Áp dụng từ được gợi ý vào text area
  const applySuggestion = (suggestedText) => {
    if (!textareaRef.current) return;
    const textarea = textareaRef.current;
    const cursor = textarea.selectionStart;
    const textBeforeCursor = sqlQuery.slice(0, cursor);
    const textAfterCursor = sqlQuery.slice(cursor);

    // Thay thế từ hiện tại đang gõ bằng từ gợi ý
    const updatedBefore = textBeforeCursor.replace(/([a-zA-Z0-9_]+)$/, suggestedText + ' ');
    const newQuery = updatedBefore + textAfterCursor;
    setSqlQuery(newQuery);
    setShowSuggestions(false);

    // Đặt lại con trỏ chuột
    setTimeout(() => {
      textarea.focus();
      const newCursorPos = updatedBefore.length;
      textarea.setSelectionRange(newCursorPos, newCursorPos);
    }, 10);
  };

  // Keyboard navigation for Autocomplete (ArrowUp, ArrowDown, Tab, Enter, Escape)
  const handleSqlKeyDown = (e) => {
    if (!showSuggestions || suggestions.length === 0) return;

    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setActiveSuggestionIdx((prev) => (prev + 1) % suggestions.length);
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setActiveSuggestionIdx((prev) => (prev - 1 + suggestions.length) % suggestions.length);
    } else if (e.key === 'Enter' || e.key === 'Tab') {
      e.preventDefault();
      applySuggestion(suggestions[activeSuggestionIdx].text);
    } else if (e.key === 'Escape') {
      setShowSuggestions(false);
    }
  };

  // Run SQL
  const handleRunSQL = async () => {
    setSqlRunning(true);
    setSqlError(null);
    setSqlResult(null);
    try {
      const res = await fetch(`${API_BASE}/sql/execute`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: sqlQuery })
      });
      const data = await res.json();
      if (!res.ok) {
        setSqlError(data.detail || 'Lỗi thực thi SQL');
      } else {
        setSqlResult(data);
      }
    } catch (e) {
      setSqlError(e.message);
    } finally {
      setSqlRunning(false);
    }
  };

  const handleInsertColumn = (colName) => {
    setSqlQuery((prev) => `${prev} ${colName}`);
  };

  const handleInsertTableSnippet = (tblName) => {
    setSqlQuery(`SELECT * FROM ${tblName} LIMIT 10;`);
  };

  const toggleTableExpand = (tblName) => {
    setExpandedTables(prev => ({
      ...prev,
      [tblName]: !prev[tblName]
    }));
  };

  const filteredSchema = schemaMetadata.filter(t => {
    if (!schemaSearch) return true;
    const s = schemaSearch.toLowerCase();
    return t.table_name.toLowerCase().includes(s) ||
           t.columns.some(c => c.name.toLowerCase().includes(s));
  });

  const filteredERD = schemaMetadata.filter(t => {
    if (erdFilter === 'core') return t.category === 'Fact' || t.category === 'Dimension';
    if (erdFilter === 'summary') return t.category === 'Summary';
    return true;
  });

  const COLORS = ['#06B6D4', '#10B981', '#F59E0B', '#8B5CF6'];

  return (
    <div className="app-container">
      {/* Sidebar */}
      <aside className="sidebar">
        <div className="sidebar-header">
          <div className="logo-badge">
            <CreditCard size={20} />
          </div>
          <div>
            <div className="sidebar-title">Credit Platform</div>
            <div className="sidebar-subtitle">Enterprise DWH & Lakehouse</div>
          </div>
        </div>

        <nav className="sidebar-menu">
          <div
            className={`menu-item ${activeTab === 'streaming' ? 'active' : ''}`}
            onClick={() => setActiveTab('streaming')}
          >
            <Activity size={18} />
            <span>Giám Sát Streaming</span>
          </div>

          <div
            className={`menu-item ${activeTab === 'gold' ? 'active' : ''}`}
            onClick={() => setActiveTab('gold')}
          >
            <TrendingUp size={18} />
            <span>Báo Cáo Tổng Hợp DWH</span>
          </div>

          <div
            className={`menu-item ${activeTab === 'erd' ? 'active' : ''}`}
            onClick={() => setActiveTab('erd')}
          >
            <Network size={18} />
            <span>Sơ Đồ Star Schema (ERD)</span>
          </div>

          <div
            className={`menu-item ${activeTab === 'sql' ? 'active' : ''}`}
            onClick={() => setActiveTab('sql')}
          >
            <Terminal size={18} />
            <span>Truy Vấn SQL Trực Tiếp</span>
          </div>

          <div
            className={`menu-item ${activeTab === 'inspector' ? 'active' : ''}`}
            onClick={() => setActiveTab('inspector')}
          >
            <TableIcon size={18} />
            <span>Duyệt Bảng Chi Tiết</span>
          </div>
        </nav>

        <div className="sidebar-footer">
          <div>Engine: <span style={{color: '#06B6D4'}}>Spark & SQLite Memory</span></div>
          <div>Backend API: <span style={{color: '#10B981'}}>Online (:8000)</span></div>
        </div>
      </aside>

      {/* Main Content */}
      <main className="main-content">
        <header className="top-navbar">
          <div className="page-title">
            {activeTab === 'streaming' && <><Activity size={20} color="#06B6D4" /> Giám Sát Giao Dịch Thời Gian Thực (POS Live)</>}
            {activeTab === 'gold' && <><TrendingUp size={20} color="#10B981" /> Báo Cáo Tổng Hợp Doanh Số & Rủi Ro (Tầng Gold)</>}
            {activeTab === 'erd' && <><Network size={20} color="#38BDF8" /> Sơ Đồ Cấu Trúc Bảng Star Schema (ERD Model)</>}
            {activeTab === 'sql' && <><Terminal size={20} color="#F59E0B" /> Trung Tâm Truy Vấn SQL & Gợi Ý Code Tự Động</>}
            {activeTab === 'inspector' && <><TableIcon size={20} color="#8B5CF6" /> Trình Duyệt Bảng Dữ Liệu Tầng Gold (Data Inspector)</>}
          </div>

          <div className="navbar-actions">
            {activeTab === 'streaming' && (
              <button
                className={`btn ${isLivePolling ? 'btn-live-active' : 'btn-outline'}`}
                onClick={() => setIsLivePolling(!isLivePolling)}
              >
                <RefreshCw size={14} className={isLivePolling ? 'animate-spin' : ''} />
                {isLivePolling ? 'Đang Live Poll (3s)' : 'Bật Tự Động Làm Mới'}
              </button>
            )}

            <button
              className="btn btn-outline"
              onClick={() => {
                fetchStreamingData();
                fetchGoldData();
                fetchSchemaMetadata();
                fetchTableList();
                if (activeTab === 'inspector') fetchTableDetail(selectedTable);
              }}
            >
              <RefreshCw size={14} />
              Làm mới
            </button>
          </div>
        </header>

        {/* Page Body */}
        <div className="page-body">
          {/* TAB 1: STREAMING REAL-TIME */}
          {activeTab === 'streaming' && (
            <>
              {/* Status bar */}
              <div style={{
                background: '#131B2A',
                borderLeft: '4px solid #06B6D4',
                borderRadius: '8px',
                padding: '12px 20px',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                fontSize: '13px'
              }}>
                <div>
                  <span style={{ color: '#94A3B8' }}>TRẠNG THÁI SPARK STREAMING: </span>
                  <span className={`badge ${streamingKpi?.is_active ? 'badge-emerald' : 'badge-rose'}`}>
                    {streamingKpi?.is_active ? 'ACTIVE (24/7)' : 'INACTIVE'}
                  </span>
                </div>
                <div>
                  <span style={{ color: '#94A3B8' }}>GIAO DỊCH MỚI NHẤT: </span>
                  <span style={{ color: '#FBBF24', fontWeight: 600 }}>{streamingKpi?.latest_tx_time || 'Chưa có dữ liệu'}</span>
                </div>
                <div>
                  <span style={{ color: '#94A3B8' }}><Clock size={13} style={{ verticalAlign: 'middle', marginRight: 4 }} />ĐỒNG HỒ HỆ THỐNG: </span>
                  <span>{systemTime}</span>
                </div>
              </div>

              {/* KPI Cards */}
              <div className="kpi-grid">
                <div className="kpi-card">
                  <div className="kpi-header">Khách hàng <Users size={16} /></div>
                  <div className="kpi-value">{streamingKpi?.total_users.toLocaleString() || 0}</div>
                  <div className="kpi-sub"><span className="badge badge-emerald">Tầng Silver</span></div>
                </div>

                <div className="kpi-card">
                  <div className="kpi-header">Thẻ tín dụng <CreditCard size={16} /></div>
                  <div className="kpi-value">{streamingKpi?.total_cards.toLocaleString() || 0}</div>
                  <div className="kpi-sub"><span className="badge badge-emerald">Tầng Silver</span></div>
                </div>

                <div className="kpi-card">
                  <div className="kpi-header">Tổng giao dịch <Layers size={16} /></div>
                  <div className="kpi-value">{streamingKpi?.total_transactions.toLocaleString() || 0}</div>
                  <div className="kpi-sub"><span className="badge badge-cyan">Parquet Lake</span></div>
                </div>

                <div className="kpi-card">
                  <div className="kpi-header">Tổng dòng tiền ($) <TrendingUp size={16} /></div>
                  <div className="kpi-value">${streamingKpi?.total_volume.toLocaleString() || '0.00'}</div>
                  <div className="kpi-sub"><span className="badge badge-amber">USD Streamed</span></div>
                </div>

                <div className="kpi-card">
                  <div className="kpi-header">Gian lận <AlertTriangle size={16} color="#F43F5E" /></div>
                  <div className="kpi-value" style={{ color: '#F87171' }}>
                    {streamingKpi?.fraud_count.toLocaleString() || 0} ({streamingKpi?.fraud_rate || 0}%)
                  </div>
                  <div className="kpi-sub"><span className="badge badge-rose">Cảnh báo</span></div>
                </div>
              </div>

              {/* Charts row */}
              <div className="grid-2col">
                <div className="card">
                  <div className="card-title">Tốc độ nạp giao dịch theo phút</div>
                  <div style={{ height: 260 }}>
                    {liveFeed.timeline.length > 0 ? (
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={liveFeed.timeline}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#1E293B" />
                          <XAxis dataKey="ts_min" stroke="#64748B" fontSize={11} />
                          <YAxis stroke="#64748B" fontSize={11} />
                          <Tooltip contentStyle={{ backgroundColor: '#0F172A', borderColor: '#334155' }} />
                          <Line type="monotone" dataKey="count" stroke="#06B6D4" strokeWidth={2} dot={{ r: 3 }} />
                        </LineChart>
                      </ResponsiveContainer>
                    ) : (
                      <div style={{ textAlign: 'center', padding: '80px 0', color: '#64748B' }}>Chờ dữ liệu luồng...</div>
                    )}
                  </div>
                </div>

                <div className="card">
                  <div className="card-title">Cơ cấu hình thức quẹt thẻ</div>
                  <div style={{ height: 260 }}>
                    {liveFeed.chip_distribution.length > 0 ? (
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie
                            data={liveFeed.chip_distribution}
                            innerRadius={60}
                            outerRadius={90}
                            paddingAngle={5}
                            dataKey="value"
                          >
                            {liveFeed.chip_distribution.map((entry, index) => (
                              <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                            ))}
                          </Pie>
                          <Tooltip contentStyle={{ backgroundColor: '#0F172A', borderColor: '#334155' }} />
                        </PieChart>
                      </ResponsiveContainer>
                    ) : (
                      <div style={{ textAlign: 'center', padding: '80px 0', color: '#64748B' }}>Chờ dữ liệu luồng...</div>
                    )}
                  </div>
                </div>
              </div>

              {/* Recent Live Feed Table */}
              <div className="card">
                <div className="card-title">Giao dịch mới nhất (Live Feed 15 records)</div>
                <div className="table-container">
                  <table className="fin-table">
                    <thead>
                      <tr>
                        <th>Thời gian</th>
                        <th>User ID</th>
                        <th>Thẻ</th>
                        <th>Số tiền</th>
                        <th>Thành phố</th>
                        <th>Bang</th>
                        <th>Hình thức</th>
                        <th>Gian lận</th>
                      </tr>
                    </thead>
                    <tbody>
                      {liveFeed.recent_transactions.map((tx, idx) => (
                        <tr key={idx}>
                          <td className="font-mono">{tx.transaction_timestamp}</td>
                          <td>{tx.user_id}</td>
                          <td>Card #{tx.card_index}</td>
                          <td style={{ fontWeight: 600 }}>${Number(tx.amount).toFixed(2)}</td>
                          <td>{tx.merchant_city || '-'}</td>
                          <td>{tx.merchant_state || '-'}</td>
                          <td><span className="badge badge-cyan">{tx.use_chip}</span></td>
                          <td>
                            {tx.is_fraud ? (
                              <span className="badge badge-rose">GIAN LẬN</span>
                            ) : (
                              <span className="badge badge-emerald">Hợp lệ</span>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </>
          )}

          {/* TAB 2: GOLD DWH ANALYTICS */}
          {activeTab === 'gold' && (
            <>
              {/* DWH KPIs */}
              <div className="kpi-grid">
                <div className="kpi-card">
                  <div className="kpi-header">Doanh số toàn hệ thống <TrendingUp size={16} /></div>
                  <div className="kpi-value">${goldKpi?.total_volume.toLocaleString() || '0.00'}</div>
                  <div className="kpi-sub"><span className="badge badge-emerald">Đối soát 100%</span></div>
                </div>

                <div className="kpi-card">
                  <div className="kpi-header">Tổng giao dịch Gold <Layers size={16} /></div>
                  <div className="kpi-value">{goldKpi?.total_transactions.toLocaleString() || 0}</div>
                  <div className="kpi-sub"><span className="badge badge-cyan">Star Schema</span></div>
                </div>

                <div className="kpi-card">
                  <div className="kpi-header">Số vụ gian lận <AlertTriangle size={16} color="#F43F5E" /></div>
                  <div className="kpi-value" style={{ color: '#F87171' }}>
                    {goldKpi?.fraud_count.toLocaleString() || 0}
                  </div>
                  <div className="kpi-sub"><span className="badge badge-rose">Đã phát hiện</span></div>
                </div>

                <div className="kpi-card">
                  <div className="kpi-header">Đối tác điểm bán (POS) <Building2 size={16} /></div>
                  <div className="kpi-value">{goldKpi?.total_merchants.toLocaleString() || 0}</div>
                  <div className="kpi-sub"><span className="badge badge-amber">Thương nhân</span></div>
                </div>
              </div>

              {/* Monthly & Quarterly Charts */}
              <div className="grid-2col">
                <div className="card">
                  <div className="card-title">Doanh số theo tháng (agg_monthly_merchant_vol)</div>
                  <div style={{ height: 280 }}>
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={monthlyTrends}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#1E293B" />
                        <XAxis dataKey="period" stroke="#64748B" fontSize={11} />
                        <YAxis stroke="#64748B" fontSize={11} />
                        <Tooltip contentStyle={{ backgroundColor: '#0F172A', borderColor: '#334155' }} />
                        <Bar dataKey="total_volume" fill="#10B981" radius={[4, 4, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>

                <div className="card">
                  <div className="card-title">Doanh số theo quý (agg_quarterly_merchant_vol)</div>
                  <div style={{ height: 280 }}>
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={quarterlyTrends}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#1E293B" />
                        <XAxis dataKey="period" stroke="#64748B" fontSize={11} />
                        <YAxis stroke="#64748B" fontSize={11} />
                        <Tooltip contentStyle={{ backgroundColor: '#0F172A', borderColor: '#334155' }} />
                        <Line type="monotone" dataKey="total_volume" stroke="#F59E0B" strokeWidth={3} dot={{ r: 4 }} />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>

              {/* Fraud Card Risk Monitor */}
              <div className="card">
                <div className="card-title">Top thẻ có dấu hiệu gian lận cao nhất (agg_daily_card_fraud)</div>
                <div className="table-container">
                  <table className="fin-table">
                    <thead>
                      <tr>
                        <th>Ngày (date_sk)</th>
                        <th>Khóa thẻ (card_sk)</th>
                        <th>Lượt quẹt</th>
                        <th>Tổng tiền ($)</th>
                        <th>Số vụ gian lận</th>
                        <th>Tỷ lệ gian lận</th>
                      </tr>
                    </thead>
                    <tbody>
                      {cardFraudRisks.map((c, i) => (
                        <tr key={i}>
                          <td className="font-mono">{c.date_sk}</td>
                          <td>Thẻ #{c.card_sk}</td>
                          <td>{c.total_txn}</td>
                          <td>${Number(c.total_amount).toFixed(2)}</td>
                          <td style={{ color: '#F87171', fontWeight: 600 }}>{c.fraud_count}</td>
                          <td><span className="badge badge-rose">{c.fraud_rate_pct}%</span></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </>
          )}

          {/* TAB 3: STAR SCHEMA ERD DIAGRAM */}
          {activeTab === 'erd' && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
              {/* ERD Controls */}
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 10 }}>
                <div style={{ display: 'flex', gap: 8 }}>
                  <button
                    className={`btn ${erdFilter === 'all' ? 'btn-primary' : 'btn-outline'}`}
                    onClick={() => setErdFilter('all')}
                  >
                    Tất Cả (9 Bảng)
                  </button>
                  <button
                    className={`btn ${erdFilter === 'core' ? 'btn-primary' : 'btn-outline'}`}
                    onClick={() => setErdFilter('core')}
                  >
                    Star Schema Lõi (Fact + 4 Dim)
                  </button>
                  <button
                    className={`btn ${erdFilter === 'summary' ? 'btn-primary' : 'btn-outline'}`}
                    onClick={() => setErdFilter('summary')}
                  >
                    Tháp Tổng Hợp (Summary Layers)
                  </button>
                </div>

                <div style={{ fontSize: 13, color: '#94A3B8', display: 'flex', alignItems: 'center', gap: 15 }}>
                  <span>Tệp HTML độc lập: <code style={{color: '#06B6D4'}}>docs/dwh_star_schema_diagram.html</code></span>
                </div>
              </div>

              {/* ERD Cards Grid matching the uploaded image */}
              <div className="erd-grid">
                {filteredERD.map((tbl) => (
                  <div key={tbl.table_name} className="erd-card">
                    <div className="erd-header" style={{
                      backgroundColor: `${tbl.color}20`,
                      color: tbl.color,
                      borderTop: `3px solid ${tbl.color}`
                    }}>
                      <span>{tbl.table_name}</span>
                      <span style={{ fontSize: 10, opacity: 0.85 }}>{tbl.category}</span>
                    </div>

                    <div className="erd-body">
                      {tbl.columns.map((col) => (
                        <div key={col.name} className="erd-col-row">
                          <span style={{ color: '#64748B', fontSize: 10 }}>{col.type}</span>
                          <span style={{ color: '#F8FAFC', fontWeight: col.key ? 600 : 400 }} title={col.desc || ''}>
                            {col.name}
                          </span>
                          <span>
                            {col.key === 'PK' && <span className="key-badge key-pk">PK</span>}
                            {col.key === 'FK' && <span className="key-badge key-fk">FK</span>}
                            {col.key === 'PK,FK' && <span className="key-badge key-pkfk">PK,FK</span>}
                            {col.key === 'NK' && <span style={{ color: '#94A3B8', fontSize: 9 }}>NK</span>}
                            {col.key === 'PIT' && <span style={{ color: '#06B6D4', fontSize: 9 }}>PIT</span>}
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* TAB 4: SQL STUDIO WITH LIVE INLINE AUTOCOMPLETE & SCHEMA HELPER */}
          {activeTab === 'sql' && (
            <div className="sql-workspace-grid">
              {/* Left Column: SQL Editor & Results */}
              <div className="card" style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
                <div className="card-title">
                  <span>Trình Soạn Thảo & Gợi Ý Code SQL (Intellisense)</span>
                  <span style={{ fontSize: 12, color: '#64748B', fontWeight: 400 }}>Engine: In-Memory SQLite kết nối Gold</span>
                </div>

                <div className="sql-editor-container">
                  {/* Preset Dropdown */}
                  <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
                    <label style={{ fontSize: 13, color: '#94A3B8' }}>Chọn câu mẫu:</label>
                    <select
                      className="select-input"
                      style={{ flex: 1 }}
                      value={selectedPreset}
                      onChange={(e) => {
                        setSelectedPreset(e.target.value);
                        setSqlQuery(SQL_PRESETS[e.target.value]);
                      }}
                    >
                      {Object.keys(SQL_PRESETS).map((k) => (
                        <option key={k} value={k}>{k}</option>
                      ))}
                    </select>
                  </div>

                  {/* Quick Snippet Pills */}
                  <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                    <span style={{ fontSize: 11, color: '#64748B', alignSelf: 'center' }}>Snippet nhanh:</span>
                    <button
                      className="btn btn-outline"
                      style={{ padding: '3px 8px', fontSize: 11 }}
                      onClick={() => setSqlQuery('SELECT * FROM fact_transactions LIMIT 10;')}
                    >
                      Fact Preview
                    </button>
                    <button
                      className="btn btn-outline"
                      style={{ padding: '3px 8px', fontSize: 11 }}
                      onClick={() => setSqlQuery('SELECT COUNT(*) AS total_records FROM fact_transactions;')}
                    >
                      Đếm dòng Fact
                    </button>
                    <button
                      className="btn btn-outline"
                      style={{ padding: '3px 8px', fontSize: 11 }}
                      onClick={() => setSqlQuery('SELECT use_chip, COUNT(*) AS cnt, SUM(amount) AS vol FROM fact_transactions GROUP BY use_chip;')}
                    >
                      Group Chip
                    </button>
                    <button
                      className="btn btn-outline"
                      style={{ padding: '3px 8px', fontSize: 11 }}
                      onClick={() => setSqlQuery('SELECT * FROM dim_cards WHERE is_current = 1 LIMIT 10;')}
                    >
                      Thẻ hiện hành (SCD2)
                    </button>
                  </div>

                  {/* SQL Text Area with Inline Autocomplete Dropdown */}
                  <div className="sql-editor-wrapper">
                    <textarea
                      ref={textareaRef}
                      className="sql-textarea"
                      value={sqlQuery}
                      onChange={handleSqlChange}
                      onKeyDown={handleSqlKeyDown}
                      placeholder="Gõ SQL vào đây... (Hệ thống tự động gợi ý từ khóa, bảng và cột khi bạn gõ!)"
                    />

                    {/* Pop-up gợi ý tự động (Inline Autocomplete Popup) */}
                    {showSuggestions && suggestions.length > 0 && (
                      <div className="autocomplete-popup">
                        <div className="autocomplete-header">
                          <span>GỢI Ý TỰ ĐỘNG (Dùng phím ↑ ↓ và Enter/Tab để chọn)</span>
                          <Sparkles size={13} color="#06B6D4" />
                        </div>
                        {suggestions.map((item, index) => (
                          <div
                            key={`${item.text}-${index}`}
                            className={`autocomplete-item ${index === activeSuggestionIdx ? 'selected' : ''}`}
                            onClick={() => applySuggestion(item.text)}
                          >
                            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                              <span style={{ fontWeight: 600 }}>{item.text}</span>
                              {item.parentTable && (
                                <span style={{ fontSize: 10, color: '#64748B' }}>({item.parentTable})</span>
                              )}
                            </div>
                            <span className={`item-kind-badge kind-${item.kind}`}>
                              {item.kind}
                            </span>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>

                  {/* Action Button */}
                  <div style={{ display: 'flex', justifyContent: 'flex-start' }}>
                    <button
                      className="btn btn-primary"
                      disabled={sqlRunning}
                      onClick={handleRunSQL}
                    >
                      <Play size={14} />
                      {sqlRunning ? 'Đang thực thi...' : 'Chạy Truy Vấn (F5)'}
                    </button>
                  </div>

                  {/* SQL Error Box */}
                  {sqlError && (
                    <div style={{
                      backgroundColor: 'rgba(239, 68, 68, 0.1)',
                      border: '1px solid #EF4444',
                      color: '#F87171',
                      padding: '12px',
                      borderRadius: 6,
                      fontSize: 13
                    }}>
                      {sqlError}
                    </div>
                  )}

                  {/* SQL Result Table */}
                  {sqlResult && (
                    <div style={{ marginTop: 8 }}>
                      <div style={{ fontSize: 13, color: '#10B981', marginBottom: 8, fontWeight: 600 }}>
                        ✓ Trả về {sqlResult.total_rows.toLocaleString()} dòng trong {sqlResult.execution_ms} ms
                      </div>

                      <div className="table-container" style={{ maxHeight: 350 }}>
                        <table className="fin-table">
                          <thead>
                            <tr>
                              {sqlResult.columns.map((c) => (
                                <th key={c}>{c}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {sqlResult.rows.map((row, i) => (
                              <tr key={i}>
                                {sqlResult.columns.map((c) => (
                                  <td key={c}>{String(row[c] ?? '')}</td>
                                ))}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  )}
                </div>
              </div>

              {/* Right Column: Schema Assistant / Tree Panel */}
              <div className="schema-panel">
                <div className="schema-panel-header">
                  <span>Trợ Lý Gợi Ý Cột & Bảng</span>
                  <Code size={16} color="#06B6D4" />
                </div>

                <div style={{ fontSize: 11, color: '#94A3B8' }}>
                  Click vào tên cột bất kỳ để chèn ngay vào câu lệnh SQL:
                </div>

                {/* Search Box */}
                <input
                  type="text"
                  className="schema-search"
                  placeholder="Tìm bảng hoặc cột..."
                  value={schemaSearch}
                  onChange={(e) => setSchemaSearch(e.target.value)}
                />

                {/* Tree of Tables and Columns */}
                <div>
                  {filteredSchema.map((tbl) => {
                    const isExpanded = expandedTables[tbl.table_name] ?? false;
                    return (
                      <div key={tbl.table_name} className="table-schema-item">
                        <div
                          className="table-schema-title"
                          style={{ borderLeftColor: tbl.color }}
                          onClick={() => toggleTableExpand(tbl.table_name)}
                        >
                          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                            {isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                            <span>{tbl.table_name}</span>
                          </div>
                          <button
                            className="quick-insert-btn"
                            title="Chèn mẫu SELECT * vào câu lệnh"
                            onClick={(e) => {
                              e.stopPropagation();
                              handleInsertTableSnippet(tbl.table_name);
                            }}
                          >
                            + SELECT
                          </button>
                        </div>

                        {isExpanded && (
                          <div className="table-schema-cols">
                            {tbl.columns.map((col) => (
                              <div
                                key={col.name}
                                className="col-tag-item"
                                title={`Click để chèn cột "${col.name}"`}
                                onClick={() => handleInsertColumn(col.name)}
                              >
                                <span style={{ fontWeight: col.key ? 600 : 400 }}>
                                  {col.name}
                                </span>
                                <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                                  <span className="col-type">{col.type}</span>
                                  {col.key && (
                                    <span style={{
                                      fontSize: 9,
                                      padding: '1px 3px',
                                      borderRadius: 2,
                                      background: col.key.includes('PK') ? '#F59E0B20' : '#06B6D420',
                                      color: col.key.includes('PK') ? '#FBBF24' : '#38BDF8'
                                    }}>
                                      {col.key}
                                    </span>
                                  )}
                                </div>
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>
          )}

          {/* TAB 5: DATA INSPECTOR */}
          {activeTab === 'inspector' && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
              {/* Catalog Pills */}
              <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
                {tableList.map((t) => (
                  <button
                    key={t.table_name}
                    className={`btn ${selectedTable === t.table_name ? 'btn-primary' : 'btn-outline'}`}
                    onClick={() => setSelectedTable(t.table_name)}
                  >
                    <TableIcon size={14} />
                    {t.table_name}
                    <span style={{
                      backgroundColor: 'rgba(0,0,0,0.3)',
                      padding: '2px 6px',
                      borderRadius: 10,
                      fontSize: 11
                    }}>
                      {t.row_count.toLocaleString()} dòng
                    </span>
                  </button>
                ))}
              </div>

              {/* Table Preview */}
              <div className="card">
                <div className="card-title">
                  <span>Xem trước bảng: <code style={{ color: '#06B6D4' }}>{selectedTable}</code></span>
                  {tableData && (
                    <span style={{ fontSize: 12, color: '#94A3B8', fontWeight: 400 }}>
                      Tổng số: {tableData.total_rows.toLocaleString()} bản ghi (Hiển thị 25 dòng đầu)
                    </span>
                  )}
                </div>

                {inspectorLoading ? (
                  <div style={{ padding: '40px', textAlign: 'center', color: '#64748B' }}>Đang nạp file Parquet...</div>
                ) : tableData ? (
                  <div className="table-container" style={{ maxHeight: 520 }}>
                    <table className="fin-table">
                      <thead>
                        <tr>
                          {tableData.columns.map((col) => (
                            <th key={col}>{col}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {tableData.rows.map((row, idx) => (
                          <tr key={idx}>
                            {tableData.columns.map((col) => (
                              <td key={col}>{String(row[col] ?? '')}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div style={{ padding: '40px', textAlign: 'center', color: '#64748B' }}>Không tìm thấy dữ liệu.</div>
                )}
              </div>
            </div>
          )}
        </div>
      </main>
    </div>
  );
}
