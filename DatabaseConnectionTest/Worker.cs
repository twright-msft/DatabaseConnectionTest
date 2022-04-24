using Microsoft.Data.SqlClient;

namespace DatabaseConnectionTest
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                int bookNumber = 1;

                var options = new SqlRetryLogicOption()
                {
                    // Tries 60 times before throwing an exception
                    NumberOfTries = 60,
                    // Preferred gap time to delay before retry
                    DeltaTime = TimeSpan.FromSeconds(1),
                    // Maximum gap time for each delay time before retry
                    MaxTimeInterval = TimeSpan.FromSeconds(3),
                };


                // Create a retry logic provider
                SqlRetryLogicBaseProvider retryLogicProvider = SqlConfigurableRetryFactory.CreateFixedRetryProvider(options);

                ConfigurationManager configManager = new ConfigurationManager();

                var connectionStringBuilder = new SqlConnectionStringBuilder();

                //Read in the DB connection string secrets injected by Kubernetes.  Reading them in this way enables creating a single username/password secret which can be shared between the SQL MI custom resource and the app.
#if DEBUG
                var username = "";
                var password = "";
#else
                var usernamePath = Path.Combine(Directory.GetCurrentDirectory(), "secrets", "username");
                var passwordPath = Path.Combine(Directory.GetCurrentDirectory(), "secrets", "password");
                var username = System.IO.File.ReadAllText(usernamePath);
                var password = System.IO.File.ReadAllText(passwordPath);
#endif

                connectionStringBuilder.UserID = username;
                connectionStringBuilder.Password = password;
                connectionStringBuilder.IntegratedSecurity = false;
                connectionStringBuilder.DataSource = "db-external-svc";
                connectionStringBuilder.InitialCatalog = "master";
                connectionStringBuilder.Encrypt = false; //Demo hack.  Don't do this at home kids!

                // Adjust these values if you like.
                connectionStringBuilder.ConnectRetryCount = 100;
                connectionStringBuilder.ConnectRetryInterval = 1;  // Seconds.

                // Leave these values as they are.
                connectionStringBuilder.ConnectTimeout = 30;

                //connection string builder -> connection string
                var connectionString = connectionStringBuilder.ToString();

                var sqlConnection = new SqlConnection(connectionString);
                sqlConnection.RetryLogicProvider = retryLogicProvider;

                using (sqlConnection)
                {
                    SqlCommand commandSelectServerName = new SqlCommand("SELECT @@SERVERNAME", sqlConnection);
                    commandSelectServerName.Connection = sqlConnection;

                    var bookInsertString = String.Format("INSERT INTO Bookstore.dbo.Book VALUES ('Some title of a book - {0}','2022-01-01','Technology', 39.95)", bookNumber);
                    var bookCountString = String.Format("SELECT COUNT(*) FROM Bookstore.dbo.Book");

                    SqlCommand commandInsertBooks = new SqlCommand(bookInsertString, sqlConnection);
                    SqlCommand commandCountBooks = new SqlCommand(bookCountString, sqlConnection);
                    
                    try
                    {
                        sqlConnection.Open();
                        _logger.LogInformation("ServerName: {0}", commandSelectServerName.ExecuteScalar());
                        _logger.LogInformation("ServerVersion: {0}", sqlConnection.ServerVersion);
                        commandInsertBooks.ExecuteScalar();
                        _logger.LogInformation("Book Count: {0}", commandCountBooks.ExecuteScalar().ToString());
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex.Message);
                    }
                }
                bookNumber++;
                await Task.Delay(1000, stoppingToken);
            }
        }
    }
}