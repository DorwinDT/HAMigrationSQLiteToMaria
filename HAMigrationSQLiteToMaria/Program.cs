using MySqlConnector;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SQLite;
using System.Linq;
using System.Text;

namespace HAMigrationSQLiteToMaria
{
	internal static class Program
	{
		private sealed class Options
		{
			public string SqlitePath;
			public string MySqlConn;
			public int ChunkSize = 5000;
			public bool Append = false;
			public List<string> AllowTables = new List<string>();
			public int MaxRowsPerBatch = 400; // safe default
		}

		private static int Main(string[] args)
		{
			try
			{
				var opt = LoadFromConfig();
				if (opt == null)
				{
					Console.Error.WriteLine("Chyba: neviem načítať konfiguráciu (SqlitePath/MySqlConn).");
					return 2;
				}

				Console.WriteLine("== HA SQLite -> MariaDB migrátor (NET48 / batch INSERT) ==");
				Console.WriteLine("SQLite: " + opt.SqlitePath);
				Console.WriteLine("MariaDB: " + opt.MySqlConn);
				Console.WriteLine("Chunk size: " + opt.ChunkSize);
				Console.WriteLine("MaxRowsPerBatch: " + opt.MaxRowsPerBatch);
				if (opt.AllowTables.Count > 0)
					Console.WriteLine("Allowlist: " + string.Join(",", opt.AllowTables));

				using (var sqlite = new SQLiteConnection(
					"Data Source=" + opt.SqlitePath + ";Read Only=True;DateTimeKind=Utc;Foreign Keys=True;"))
				{
					sqlite.Open();

					using (var mysql = new MySqlConnection(opt.MySqlConn))
					{
						mysql.Open();

						// POZOR: Schému v MariaDB najprv nech vytvorí HA (prázdna DB -> pripojiť -> reštart),
						// potom HA vypnúť a spustiť migráciu.

						var sqliteTables = GetSqliteTables(sqlite);
						var mysqlTables = GetMySqlTables(mysql);

						var common = sqliteTables.Intersect(mysqlTables, StringComparer.OrdinalIgnoreCase)
												 .OrderBy(t => t, StringComparer.OrdinalIgnoreCase)
												 .ToList();
						if (opt.AllowTables.Count > 0)
							common = common.Where(t => opt.AllowTables.Contains(t, StringComparer.OrdinalIgnoreCase)).ToList();

						if (common.Count == 0)
						{
							Console.WriteLine("Žiadne spoločné tabuľky. Končím.");
							return 3;
						}

						Console.WriteLine("Spoločné tabuľky: " + string.Join(", ", common));

						if (!opt.Append)
						{
							Console.WriteLine("TRUNCATE cieľových tabuliek...");
							WithForeignKeysDisabled(mysql, delegate (MySqlConnection c)
							{
								foreach (var t in common)
								{
									using (var cmd = c.CreateCommand())
									{
										cmd.CommandText = "TRUNCATE TABLE " + Backtick(t);
										cmd.ExecuteNonQuery();
									}
								}
							});
						}

						foreach (var table in common)
						{
							var start = DateTime.UtcNow;

							Console.WriteLine();
							Console.WriteLine("=== " + table + " ===");
							long tableTotal = GetSqliteRowCount(sqlite, table);
							Console.WriteLine("  Celkový počet riadkov v SQLite: " + tableTotal);

							var srcCols = GetSqliteColumns(sqlite, table);
							var dstCols = GetMySqlColumns(mysql, table);
							var commonCols = srcCols.Intersect(dstCols, StringComparer.OrdinalIgnoreCase)
													.OrderBy(c => c, StringComparer.OrdinalIgnoreCase)
													.ToList();

							if (commonCols.Count == 0)
							{
								Console.WriteLine("  -> Žiadny prienik stĺpcov, preskakujem.");
								continue;
							}

							Console.WriteLine("  Stĺpce: " + string.Join(", ", commonCols));
							WithForeignKeysDisabled(mysql, delegate (MySqlConnection c)
							{
								long total = 0;
								long offset = 0;

								// vyrátaj bezpečný batch limit podľa počtu stĺpcov (MySQL limit ~65535 parametrov)
								int safeMaxRows = Math.Max(1, Math.Min(opt.MaxRowsPerBatch, 60000 / Math.Max(1, commonCols.Count)));

								while (true)
								{
									using (var readCmd = sqlite.CreateCommand())
									{
										readCmd.CommandText = string.Format(
											"SELECT {0} FROM {1} LIMIT @lim OFFSET @off",
											string.Join(", ", commonCols.Select(DoubleQuoteSqlite)),
											DoubleQuoteSqlite(table));

										readCmd.Parameters.AddWithValue("@lim", opt.ChunkSize);
										readCmd.Parameters.AddWithValue("@off", offset);

										using (var reader = readCmd.ExecuteReader(CommandBehavior.SequentialAccess))
										{
											if (!reader.HasRows)
												break;

											// Naplnenie DataTable (kvôli jednoduchšiemu batchovaniu)
											var dt = new DataTable(table);
											foreach (var col in commonCols) dt.Columns.Add(col, typeof(object));

											int readCount = 0;
											while (reader.Read())
											{
												var row = dt.NewRow();
												for (int i = 0; i < commonCols.Count; i++)
												{
													object val = reader.IsDBNull(i) ? (object)DBNull.Value : reader.GetValue(i);
													row[i] = val;
												}
												dt.Rows.Add(row);
												readCount++;
											}

											if (readCount == 0) break;

											using (var tx = c.BeginTransaction())
											{
												InsertBatches(c, tx, table, commonCols, dt, safeMaxRows);
												tx.Commit();
											}

											total += readCount;
											offset += readCount;

											var now = DateTime.UtcNow;
											var elapsed = now - start;
											var pct = tableTotal > 0 ? (total * 100.0 / tableTotal) : 0.0;
											var speed = elapsed.TotalSeconds > 0 ? total / elapsed.TotalSeconds : 0; // rows/s
											var remain = speed > 0 ? TimeSpan.FromSeconds((tableTotal - total) / speed) : TimeSpan.Zero;

											Console.WriteLine($"  + {readCount} (total {total} / {tableTotal}, {pct:0.0}% | {speed:0} r/s | ETA {remain:hh\\:mm\\:ss})");

											if (readCount < opt.ChunkSize) break;
										}
									}
								}

								Console.WriteLine("  Done: " + table);
							});
						}

						Console.WriteLine();
						Console.WriteLine("== MIGRATION DONE ==");
						Console.WriteLine("Run HA and check Recorder/history.");
					}
				}

				return 0;
			}
			catch (Exception ex)
			{
				Console.Error.WriteLine("Error: " + ex);
				return 1;
			}
		}

		// ---------- CONFIG ----------
		private static Options LoadFromConfig()
		{
			var o = new Options();
			o.SqlitePath = Properties.Settings.Default.SqlitePath;
			o.MySqlConn = Properties.Settings.Default.MySqlConn;

			o.ChunkSize = Properties.Settings.Default.ChunkSize; 

			o.Append = Properties.Settings.Default.Append;

			int mrpb;
			if (int.TryParse(ConfigurationManager.AppSettings["MaxRowsPerBatch"], out mrpb) && mrpb > 0) o.MaxRowsPerBatch = mrpb;

			var tablesCsv = (Properties.Settings.Default.Tables ?? "").Trim();
			if (tablesCsv.Length > 0)
				o.AllowTables = tablesCsv.Split(',').Select(s => (s ?? "").Trim()).Where(s => s.Length > 0).ToList();

			if (string.IsNullOrWhiteSpace(o.SqlitePath) || string.IsNullOrWhiteSpace(o.MySqlConn))
				return null;

			return o;
		}

		// ---------- INSERT BATCHING ----------
		private static void InsertBatches(MySqlConnection conn, MySqlTransaction tx, string table, List<string> cols, DataTable dt, int maxRows)
		{
			// Pozor na max_allowed_packet; ak by si mal veľmi veľké event_data, zníž maxRows
			int total = dt.Rows.Count;
			int idx = 0;

			var colList = string.Join(", ", cols.Select(Backtick));

			while (idx < total)
			{
				int take = Math.Min(maxRows, total - idx);

				var sb = new StringBuilder();
				sb.Append("INSERT INTO ").Append(Backtick(table)).Append(" (").Append(colList).Append(") VALUES ");

				var cmd = conn.CreateCommand();
				cmd.Transaction = tx;

				for (int r = 0; r < take; r++)
				{
					if (r > 0) sb.Append(",");
					sb.Append("(");

					for (int c = 0; c < cols.Count; c++)
					{
						if (c > 0) sb.Append(",");
						string p = "@p" + r + "_" + c;
						sb.Append(p);

						object val = dt.Rows[idx + r][c];
						if (val == null || val is DBNull) val = DBNull.Value;
						cmd.Parameters.AddWithValue(p, val);
					}

					sb.Append(")");
				}

				cmd.CommandText = sb.ToString();
				cmd.ExecuteNonQuery();

				idx += take;
			}
		}

		// ---------- DB HELPERS ----------
		private static string Backtick(string ident) { return "`" + ident.Replace("`", "``") + "`"; }
		private static string DoubleQuoteSqlite(string ident) { return "\"" + ident.Replace("\"", "\"\"") + "\""; }

		private static HashSet<string> GetSqliteTables(SQLiteConnection conn)
		{
			var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
			using (var cmd = conn.CreateCommand())
			{
				cmd.CommandText = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';";
				using (var rd = cmd.ExecuteReader())
				{
					while (rd.Read())
						set.Add(rd.GetString(0));
				}
			}
			return set;
		}

		private static HashSet<string> GetMySqlTables(MySqlConnection conn)
		{
			var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
			using (var cmd = conn.CreateCommand())
			{
				cmd.CommandText = "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE();";
				using (var rd = cmd.ExecuteReader())
				{
					while (rd.Read())
						set.Add(rd.GetString(0));
				}
			}
			return set;
		}

		private static HashSet<string> GetSqliteColumns(SQLiteConnection conn, string table)
		{
			var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
			using (var cmd = conn.CreateCommand())
			{
				cmd.CommandText = "PRAGMA table_info(" + DoubleQuoteSqlite(table) + ");";
				using (var rd = cmd.ExecuteReader())
				{
					while (rd.Read())
					{
						// PRAGMA table_info: column #1 = name
						set.Add(rd.GetString(1));
					}
				}
			}
			return set;
		}

		private static HashSet<string> GetMySqlColumns(MySqlConnection conn, string table)
		{
			var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
			using (var cmd = conn.CreateCommand())
			{
				cmd.CommandText = "SELECT column_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = @t;";
				cmd.Parameters.AddWithValue("@t", table);
				using (var rd = cmd.ExecuteReader())
				{
					while (rd.Read())
						set.Add(rd.GetString(0));
				}
			}
			return set;
		}

		private static void WithForeignKeysDisabled(MySqlConnection conn, Action<MySqlConnection> action)
		{
			using (var off = conn.CreateCommand())
			{
				off.CommandText = "SET FOREIGN_KEY_CHECKS=0;";
				off.ExecuteNonQuery();
			}
			try
			{
				action(conn);
			}
			finally
			{
				using (var on = conn.CreateCommand())
				{
					on.CommandText = "SET FOREIGN_KEY_CHECKS=1;";
					on.ExecuteNonQuery();
				}
			}
		}

		private static long GetSqliteRowCount(SQLiteConnection conn, string table)
		{
			using (var cmd = conn.CreateCommand())
			{
				cmd.CommandText = "SELECT COUNT(*) FROM " + DoubleQuoteSqlite(table) + ";";
				object val = cmd.ExecuteScalar();
				return (val == null || val == DBNull.Value) ? 0 : Convert.ToInt64(val);
			}
		}

	}
}
