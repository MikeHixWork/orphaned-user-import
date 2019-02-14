using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace ImportOrphanedUsers
{
    class Program
    {
        static void Main(string[] args)
        {
            AddNewUsersToUserTable();
        }

        public static void AddNewUsersToUserTable()
        {
            // Currently (09/2018), the "lax" user status is not used for anything at all.
            // No user records are flagged with this status.  So, use it here to flag users
            // that are being "machine generated" by this app.  "Lax" is actually not an
            // inappropriate designation.
            const int USER_STATUS_LAX = 3;      // see: NPEC40.dbo.tblUserStatus
            List<BHGUser> reviewUsers = new List<BHGUser>();
            const int CURRENT_IMPORT_SOURCE_INT = 2;
            const string CURRENT_IMPORT_SOURCE_STRING = "bhg - 20190205 import";


            using (StreamReader r = new StreamReader("BHGUsersFromReviews.json"))
            {
                string jsonFileContents = r.ReadToEnd();
                reviewUsers = JsonConvert.DeserializeObject<List<BHGUser>>(jsonFileContents);
            }

            using (SqlConnection connection = ConnectNpec40())
            {
                string commandTxt =
                    "IF NOT EXISTS (SELECT * FROM [NPEC40].[dbo].[tblUserImportSource] " +
                   $"    WHERE [i_UserImportSourceId] = {CURRENT_IMPORT_SOURCE_INT}) " +
                    "INSERT INTO npec40.dbo.tblUserImportSource " +
                    $"VALUES ({CURRENT_IMPORT_SOURCE_INT}, '{CURRENT_IMPORT_SOURCE_STRING}')";
                SqlCommand cmd = new SqlCommand(commandTxt, connection);
                cmd.CommandType = CommandType.Text;

                int numRowsAffected = cmd.ExecuteNonQuery();

                if (numRowsAffected < 0)   // nothing happened? -1 = INSERT was not attempted at all
                {
                    string message = $"Source with 'i_UserImportSourceId' = \"{CURRENT_IMPORT_SOURCE_INT}\" already existed in tblUser, "
                        + "so no INSERT was done.";

                    LogWriter?.WriteLine(message);
                    Console.WriteLine(message);
                }

                if (numRowsAffected == 0)   // nothing happened? 0 = INSERT did not insert anything (for some reason)
                {
                    string message = $"FAILED: Source with 'i_UserImportSourceId' = \"{CURRENT_IMPORT_SOURCE_INT}\" was not INSERTed "
                        + "into tblUser: a SQL error must have occurred.";

                    LogWriter?.WriteLine(message);
                    Console.WriteLine(message);
                }
            }

            foreach (var user in reviewUsers)
            {
                string username = String.IsNullOrWhiteSpace(user.DisplayName) ? "anonymous" : user.DisplayName;
                username = username.Replace("'", "''");
                string userId = user.UserId;

                // Only want to process users that are in GUID form.
                if (userId.Contains('-'))
                {
                    Guid userGuid = Guid.Parse(userId);
                    // Aside: by not specifying the i_EmailAddressID here, it defaults to '2', which 
                    // is a dummy account named "nobody@allrecipes.com".
                    string commandText =
                        "IF NOT EXISTS (SELECT * FROM [NPEC40].[dbo].[tblUser] " +
                       $"    WHERE [UserGuid] = '{userGuid}') " +
                        "BEGIN " +
                        "INSERT INTO [NPEC40].[dbo].[tblUser] " +   // the INSERT statement causes a GUID to be generated for this (new) user record (CHECK THAT CAN BE WRITTEN TO)
                        "( " +
                        "    [vch_DisplayName], " +
                        "    [i_UserStatusID], " +
                        "    [UserGuid], " +
                        "    [i_RatingReviewCount], " +
                        "    [i_UserImportSource] " +
                        ") " +
                        "VALUES " +
                        "( " +
                       $"    '{username}', " +
                       $"    {USER_STATUS_LAX}, " +             // flag all "machine generated" users with the "Lax" status
                       $"    '{userGuid}', " +
                        "    1, " +                             // needs to get set so that the daemon will pick up the users for the user-card-cache
                       $"    '{CURRENT_IMPORT_SOURCE_INT}' " +
                        ") " +
                        "END ";
                    using (SqlConnection connection = ConnectNpec40())
                    {
                        SqlCommand cmd = new SqlCommand(commandText, connection);
                        cmd.CommandType = CommandType.Text;


                        // 'ExecuteNonQuery()' returns the number of rows affected.  See:
                        //      https://stackoverflow.com/questions/21602070/executenonquery-returns-what-int-value
                        //
                        // HOWEVER: I have discovered (not documented anywhere that I know of), that
                        // because of the "IF NOT EXISTS ..." wrapper, then if the "IF" is false, the
                        // "INSERT" is not even attempted (of course); so in that case, the number of
                        // rows affected is -1, not 0.
                        int numRowsAffected = cmd.ExecuteNonQuery();

                        if (numRowsAffected < 0)   // nothing happened? -1 = INSERT was not attempted at all
                        {
                            string message = $"User with 'UserGuid' = \"{userGuid}\" already existed in tblUser, "
                                + "so no INSERT was done.";

                            LogWriter?.WriteLine(message);
                            Console.WriteLine(message);
                        }

                        if (numRowsAffected == 0)   // nothing happened? 0 = INSERT did not insert anything (for some reason)
                        {
                            string message = $"FAILED: User with 'UserGuid' = \"{userGuid}\" was not INSERTed "
                                + "into tblUser: a SQL error must have occurred.";

                            LogWriter?.WriteLine(message);
                            Console.WriteLine(message);
                        }
                    }
                }
            }
        }

        public static SqlCommand CreateCommand(string SprocName, SqlConnection Connection)
        {
            SqlCommand ret = new SqlCommand();
            ret.Connection = Connection;
            ret.CommandType = CommandType.StoredProcedure;
            ret.CommandText = SprocName;
            ret.CommandTimeout = 60;
            return ret;
        }

        // NOTE: this builds a SqlConnection and does *not* dispose of it in any way.
        // If you repeatedly call this method (in a loop), you eat up the entire pool of
        // available connections.  Don't do that.  If you have a loop, use a
        // "using (SqlConnection foo = ConnectNpec40()) { ..." construct.
        public static SqlConnection ConnectNpec40()
        {
            SqlConnection ret = new SqlConnection();
            ret.ConnectionString = Npec40Connection;
            ret.Open();

            return ret;
        }

        // List of database names:
        // C:\Git\Source\Allrecipes.Data\src\DatabasesNames.cs
        private static string Npec40Connection
        {
            get { return Allrecipes.Data.ConnectionManager.GetConnectionString("Npec40"); }
        }

        /// <summary>
        /// Convert a DataColumn "string" column into an actual C# string.
        /// </summary>
        public static string ToTrimmedString(object dataColumn)
        {
            string result = dataColumn as string;
            if (result != null)
            {
                result = result.Trim();
            }
            return result;
        }

        #region Logging

        // LogWriter = new StreamWriter(Console.OpenStandardOutput());      // log to the Console
        private static StreamWriter _logWriter = null;

        public static StreamWriter LogWriter
        {
            get { return _logWriter; }

            set
            {
                _logWriter = value;

                if (_logWriter == null)
                {
                    return;
                }

                _logWriter.AutoFlush = true;     // essential! Otherwise, closing the stream can easily truncate the output.

                bool canWrite = (_logWriter.BaseStream != null);  // https://stackoverflow.com/questions/11323808/how-can-i-tell-if-a-streamwriter-is-closed
                if (canWrite)
                {
                    _logWriter.WriteLine($" » Logfile (re)opened at {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}");
                }
            }
        }

        /// <summary>
        /// Check to see if this run of the app should write to local log files.  "Local"
        /// means, log files on the local hard disk, rather than "remote" logging like
        /// Splunk.  Local logging is enabled by suitable entries in App.config.
        /// </summary>
        private static StreamWriter CheckIfWantLocalLogging()
        {
            bool enableLocalLogging =
                bool.TryParse(ConfigurationManager.AppSettings["EnableLocalLogging"], out enableLocalLogging)
                && enableLocalLogging;

            // REMARK: examples of valid folder strings:
            //      "C:\Foo\MyTempDirectory"
            //      "."         (the directory containing the executable app)
            //      ".\Logs"    (create a 'Logs' folder, below the directory containing the executable app)
            string outputFolderForLogFiles = ConfigurationManager.AppSettings["OutputFolderForLogFiles"];

            if (enableLocalLogging && !string.IsNullOrWhiteSpace(outputFolderForLogFiles))
            {
                string outputFolder = outputFolderForLogFiles.TrimEnd('\\');   // ensure consistent format
                Directory.CreateDirectory(outputFolder);    // create directory (and sub) paths, if not exist

                string outputLogFileName = $"{Process.GetCurrentProcess().ProcessName}_Log_{DateTime.Now.ToString("yyyy-MM-dd")}.txt";

                return new StreamWriter($"{outputFolder}\\{outputLogFileName}", true);   // true: append if exists, else create new
            }

            return null;
        }

        #endregion  // Logging

        class BHGUser
        {
            public string UserId { get; set; }
            public string DisplayName { get; set; }
        }
    }
}
