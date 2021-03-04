using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.IO;
using System.Threading;

namespace Taxmann
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();

        }
        private static FileStream fs = new FileStream(@"c:\temp\mcb.txt", FileMode.OpenOrCreate, FileAccess.Write);
        private static StreamWriter m_streamWriter = new StreamWriter(fs);
        private void button1_Click(object sender, EventArgs e)
        {
            var abhi = new System.Diagnostics.Stopwatch();
            abhi.Start();
            OleDbConnection con = new OleDbConnection();
            string dbProvider = "PROVIDER=Microsoft.Jet.OLEDB.4.0;";
            string dbSource = "Data Source = C:/Users/Abhishek/Desktop/demo.mdb";
            con.ConnectionString = dbProvider + dbSource;
            con.Open();
            OleDbCommand cmd = new OleDbCommand();
            cmd.Connection = con;
            cmd.CommandText = "INSERT INTO Emp (Title, Designation, EMployeName) VALUES (?,?,?)";
            cmd.Parameters.Add("?", OleDbType.VarWChar, 255);
            cmd.Parameters.Add("?", OleDbType.VarWChar, 255);
            cmd.Parameters.Add("?", OleDbType.VarWChar, 255);
            cmd.Prepare();
            cmd.Parameters[0].Value = "Du1";
            cmd.Parameters[2].Value = "Du2";
            cmd.Parameters[3].Value = "Du3";
            OleDbTransaction trn = con.BeginTransaction();
            cmd.Transaction = trn;
            for (int i = 0; i < 100000; i++)
            {
                cmd.ExecuteNonQuery();
            }
            OleDbCommand cmd1 = new OleDbCommand();
            cmd1.Connection = con;
            cmd1.CommandText = "INSERT INTO Book (Title, BookName, Author) VALUES (?,?,?)";
            cmd1.Parameters.Add("?", OleDbType.VarWChar, 255);
            cmd1.Parameters.Add("?", OleDbType.VarWChar, 255);
            cmd1.Parameters.Add("?", OleDbType.VarWChar, 255);
            cmd1.Prepare();
            cmd1.Parameters[0].Value = "Du4";
            cmd1.Parameters[2].Value = "Du5";
            cmd1.Parameters[3].Value = "Du6";
            OleDbTransaction trn1 = con.BeginTransaction();
            cmd1.Transaction = trn1;
            for (int i = 0; i < 100000; i++)
            {
                cmd1.ExecuteNonQuery();
            }
            OleDbCommand cmd2 = new OleDbCommand();
            cmd2.Connection = con;
            cmd2.CommandText = "INSERT INTO Manager (Title, ManagName, Level) VALUES (?,?,?)";
            cmd2.Parameters.Add("?", OleDbType.VarWChar, 255);
            cmd2.Parameters.Add("?", OleDbType.VarWChar, 255);
            cmd2.Parameters.Add("?", OleDbType.VarWChar, 255);
            cmd2.Prepare();
            cmd2.Parameters[0].Value = "Du7";
            cmd2.Parameters[2].Value = "Du8";
            cmd2.Parameters[3].Value = "Du9";
            OleDbTransaction trn2 = con.BeginTransaction();
            cmd2.Transaction = trn2;
            for (int i = 0; i < 100000; i++)
            {
                cmd2.ExecuteNonQuery();
            }
            trn.Commit();
            con.Close();
            abhi.Stop();
            //(String.Format("{0:0.0} seconds", abhi.ElapsedMilliseconds / 1000.0));//
            timer1.Enabled = true;
            textBox1.Text = System.Windows.Forms.SystemInformation.ComputerName;
            Thread.MemoryBarrier();
            var initialMemory = System.GC.GetTotalMemory(true);
            // body
            var somethingThatConsumesMemory = Enumerable.Range(0, 100000)
                .ToArray();
            // end
            Thread.MemoryBarrier();
            var finalMemory = System.GC.GetTotalMemory(true);
            var consumption = finalMemory - initialMemory;
            OperatingSystem os = Environment.OSVersion;
            Version ver = os.Version;

            GetData();


        }

        private void GetData()
        {
            string connectionString = GetConnectionString();
            // Open a sourceConnection to the AdventureWorks database.
            using (SqlConnection sourceConnection =
                       new SqlConnection(connectionString))
            {
                sourceConnection.Open();

                // Perform an initial count on the destination table.
                SqlCommand commandRowCount = new SqlCommand(
                    "SELECT COUNT(*) FROM " +
                    "dbo.Emp;",
                    sourceConnection);
                long countStart = System.Convert.ToInt32(
                    commandRowCount.ExecuteScalar());
                Console.WriteLine("Starting row count = {0}", countStart);

                // Get data from the source table as a SqlDataReader.
                SqlCommand commandSourceData = new SqlCommand(" select Title, Designation, EMployeName from emp", sourceConnection);
                SqlDataReader reader =
                    commandSourceData.ExecuteReader();
                SqlCommand commandSourceData1 = new SqlCommand(" select Title, BookName, Author from book", sourceConnection);
                SqlDataReader reader1 =
                    commandSourceData1.ExecuteReader();
                SqlCommand commandSourceData2 = new SqlCommand(" select Title, ManagName, Level from Manager", sourceConnection);
                SqlDataReader reader2 =
                    commandSourceData2.ExecuteReader();

                // Open the destination connection. In the real world you would 
                // not use SqlBulkCopy to move data from one table to the other 
                // in the same database. This is for demonstration purposes only.
                using (SqlConnection destinationConnection =
                           new SqlConnection(connectionString))
                {
                    destinationConnection.Open();

                    // Set up the bulk copy object. 
                    // Note that the column positions in the source
                    // data reader match the column positions in 
                    // the destination table so there is no need to
                    // map columns.
                    //------------First table////
                    using (SqlBulkCopy bulkCopy =
                               new SqlBulkCopy(destinationConnection))
                    {
                        bulkCopy.DestinationTableName =
                            "dbo.Emp";

                        try
                        {
                            // Write from the source to the destination.
                            bulkCopy.WriteToServer(reader);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                        }
                        finally
                        {
                            // Close the SqlDataReader. The SqlBulkCopy
                            // object is automatically closed at the end
                            // of the using block.
                            reader.Close();
                        }
                    }
                    //------------Second table////
                    // Perform a final count on the destination 
                    // table to see how many rows were added.
                    long countEnd = System.Convert.ToInt32(
                        commandRowCount.ExecuteScalar());
                    using (SqlBulkCopy bulkCopy1 =
                               new SqlBulkCopy(destinationConnection))
                    {
                        bulkCopy1.DestinationTableName =
                            "dbo.book";

                        try
                        {
                            // Write from the source to the destination.
                            bulkCopy1.WriteToServer(reader1);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                        }
                        finally
                        {
                            // Close the SqlDataReader. The SqlBulkCopy
                            // object is automatically closed at the end
                            // of the using block.
                            reader1.Close();
                        }
                    }

                    // Perform a final count on the destination 
                    // table to see how many rows were added.
                    long countEnd1 = System.Convert.ToInt32(
                        commandRowCount.ExecuteScalar());


                    //------------Third table //
                    using (SqlBulkCopy bulkCopy2 =
                               new SqlBulkCopy(destinationConnection))
                    {
                        bulkCopy2.DestinationTableName =
                            "dbo.Manager";

                        try
                        {
                            // Write from the source to the destination.
                            bulkCopy2.WriteToServer(reader);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                        }
                        finally
                        {
                            // Close the SqlDataReader. The SqlBulkCopy
                            // object is automatically closed at the end
                            // of the using block.
                            reader.Close();
                        }
                    }

                    // Perform a final count on the destination 
                    // table to see how many rows were added.
                    long countEnd2 = System.Convert.ToInt32(
                        commandRowCount.ExecuteScalar());

                }

            }
        }

        private string GetConnectionString()
        {
            return "Data Source=(local); " +
            " Integrated Security=true;" +
            "Initial Catalog=demo;";
        }

        private void timer1_Tick(object sender, EventArgs e)
        {
            m_streamWriter.WriteLine("{0} {1}",
          DateTime.Now.ToLongTimeString(), DateTime.Now.ToLongDateString());
            m_streamWriter.Flush();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            // Write to the file using StreamWriter class    
            m_streamWriter.BaseStream.Seek(0, SeekOrigin.End);
            m_streamWriter.Write(" File Write Operation Starts : ");
            m_streamWriter.WriteLine("{0} {1}",
            DateTime.Now.ToLongTimeString(), DateTime.Now.ToLongDateString());
            m_streamWriter.WriteLine("===================================== \n");
            m_streamWriter.Flush();
        }
    }
    
}
