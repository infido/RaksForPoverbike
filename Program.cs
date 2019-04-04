using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Topshelf;
using Topshelf.Logging;
using System.Timers;
using Microsoft.Win32;
using FirebirdSql.Data.FirebirdClient;
using System.IO;
using System.Net;

namespace RaksForPoverbike
{
    class Program
    {
        static void Main(string[] args)
        {
            SettingFile st = new SettingFile(true);
            HostFactory.Run(x =>                                
            {

                x.ApplyCommandLine();
                
                x.Service<Wysylacz>(s =>                        
                {
                    s.ConstructUsing(name => new Wysylacz());     
                    s.WhenStarted(tc => tc.Start());             
                    s.WhenStopped(tc => tc.Stop());               
                });
                x.RunAsLocalSystem();
                x.StartAutomatically();

                x.SetDescription("Usługa atumatycznego wysyłania raportów do Powerbike");        
                x.SetDisplayName("Wysyłacz na FTP do POWERBIKE");                       
                x.SetServiceName("Wysylacz_ftp_PB");
            });
  
            
        }
    }

    public class Wysylacz
    {
        public static FbConnection conn;
        private string pathInfo;

        readonly Timer _timer;
        public Wysylacz()
        {
            //1 sekunda 
            //_timer = new Timer(1000) { AutoReset = true };

            //5 minut
            //_timer = new Timer(1000 * 60 * 5) { AutoReset = true };

            //1 godzina
            _timer = new Timer(1000 * 60 * 60) { AutoReset = true };
            
            _timer.Elapsed += (sender, eventArgs) => logowanieDoPliku("Wysyłacz wykonuje się w pętli...", "INFO");
            _timer.Elapsed += new ElapsedEventHandler(this.wyslanie_do_ftp);

        }
        public void Start() 
        {
            _timer.Start(); 
        }
        public void Stop() { _timer.Stop(); }

        public void wyslanie_do_ftp(object sender, EventArgs e)
        {
            //Uruchomienie stanu przejściowego od 2019-04-03 zmiana generowania godziny plików na po 12 w południe
            if (
                (DateTime.Now.Hour >= 22 && DateTime.Now.Hour < 23 && DateTime.Now.DayOfWeek == DayOfWeek.Sunday)
                ||
                (DateTime.Now.Hour >= 22 && DateTime.Now.Hour < 23 && DateTime.Now.DayOfWeek == DayOfWeek.Wednesday)
                )
            {
                logowanieDoPliku("Łaczymy się z FB (wyslanie_do_ftp)", "INFO");
                setConnectionON(true);
                logowanieDoPliku("Po połączeniu się z FB (wyslanie_do_ftp)", "INFO");

                //wykonanie zapytań 
                logowanieDoPliku("+++++++++++++++++++++++++++++++++++++++++++ Przygotowanie RAPORTU KRAK", "INFO");
                przygotowanieRaportu("'KRAK'", "", "", "'POWERBIKE'", "", true, true);
                logowanieDoPliku("+++++++++++++++++++++++++++++++++++++++++++ Przygotowanie RAPORTU WESO", "INFO");
                przygotowanieRaportu("'WESO'", "", "", "'POWERBIKE'", "", true, true);
                logowanieDoPliku("+++++++++++++++++++++++++++++++++++++++++++ Przygotowanie RAPORTU PRZE", "INFO");
                przygotowanieRaportu("'PRZE'", "", "", "'POWERBIKE'", "", true, true);
                logowanieDoPliku("+++++++++++++++++++++++++++++++++++++++++++ Przygotowanie RAPORTU CENTR + NOWY do jednego pliku", "INFO");
                przygotowanieRaportu("'CENTR','NOWY'", "", "", "'POWERBIKE'", "", true, true);
                logowanieDoPliku("+++++++++++++++++++++++++++++++++++++++++++ Przygotowanie RAPORTU WARS", "INFO");
                przygotowanieRaportu("'WARS'", "", "", "'POWERBIKE'", "", true, true);
                //logowanieDoPliku("+++++++++++++++++++++++++++++++++++++++++++ Przygotowanie RAPORTU NOWY", "INFO");
                //przygotowanieRaportu("'NOWY'", "", "", "'POWERBIKE'", "", true, true);
                
                logowanieDoPliku("+++++++++++++++++++++++++++++++++++++++++++KONIEC RAPORTU", "INFO");
                
                setConnectionOFF();
            }
            else
            {
                logowanieDoPliku("Negatywna kontrola czasu, nie wykonanie operacji", "CHECK");
            }
        }

        public string getConnectionString()
        {
            logowanieDoPliku("Definicja połączenia do FB START", "INFO");
            SettingFile ustawieniaApp = new SettingFile();
            logowanieDoPliku("Definicja połączenia do FB odczytano ustawienia z pliku", "INFO");
            string connectionString = "";
            try
            {
                connectionString =
                    "User=SYSDBA;" +
                    "Password=masterkey;" +
                    "Database=" + ustawieniaApp.Database + ";" +
                    //"Database=" + "/usr/raks/Data/F00001.fdb;" +
                    "Datasource="  + ustawieniaApp.DataSourcePath + ";" +
                    //"Datasource=10.0.0.100;" +
                    "Port=3050;" +
                    "Dialect=3;" +
                    //"Charset=NONE;" +
                    "Charset=WIN1250;" +
                    "Role=;" +
                    "Connection lifetime=15;" +
                    "Pooling=true;" +
                    "MinPoolSize=0;" +
                    "MaxPoolSize=50;" +
                    "Packet Size=8192;" +
                    "ServerType=0";

                pathInfo = ustawieniaApp.DataSourcePath + ":" + ustawieniaApp.Database;
                logowanieDoPliku("utworzono definicje połączenia do FB: " + pathInfo, "INFO");
                //logowanieDoPliku("utworzono definicje połączenia do FB connection string: " + connectionString, "INFO");
            }
            catch (Exception ex)
            {
                logowanieDoPliku("Bład tworzenia połączenia do FB: " + ex.Message, "ERROR");
                throw new System.ArgumentException("Bład tworzenia połączenia do FB: " + ex.Message, "original");
                //Logg logg = new Logg(Logg.RodzajLogowania.ErrorMSG, Logg.MediumLoga.File, "1002: Błąd odczytu constr z rejestrze Windows: " + ex.Message);
                //System.Windows.Forms.MessageBox.Show("1002: Błąd odczytu constr z rejestrze Windows: " + ex.Message);
            }

            return connectionString;
        }

        private void setConnectionON(Boolean _trybTest)
        {
            logowanieDoPliku("setConnectionON (Program)", "INFO");
            try
            {
                conn = new FbConnection(getConnectionString());
            }
            catch (FbException fb)
            {
                logowanieDoPliku("9000: Błąd tworzenia połaczenia do Firebird: " + fb.Message, "ERROR");
            }
            logowanieDoPliku("9001: Ustawiono parametry połaczenia. ","INFO");

            try
            {
                conn.Open();
                if (conn.State > 0)
                {
                    if (_trybTest)
                    {
                        logowanieDoPliku("9002: Nawiązano połaczenie. " + conn.Database + " Status=" + conn.State, "INFO");
                    }
                    else
                    {
                        logowanieDoPliku("9003: Nawiązano połaczenie! " + conn.Database + " Status=" + conn.State,"INFO");
                    }
                }
                else
                {
                    if (_trybTest)
                    {
                        logowanieDoPliku("1003: Nie połączono! Status=" + conn.State,"ERROR");
                    }
                    else
                    {
                        logowanieDoPliku("1004: Błąd połączenia z bazą!","ERROR");
                    }
                }
            }
            catch (Exception ex)
            {
                if (_trybTest)
                {
                    logowanieDoPliku("1005: Błąd: " + ex.Message,"ERROR");
                }
                else
                {
                    logowanieDoPliku("1006: Błąd: " + ex.Message, "ERROR");
                }
            }
        }

        public void setConnectionOFF()
        {
            conn.Close();
            logowanieDoPliku("9003: Rozłaczono! Status=" + conn.State, "INFO");
        }

        public void przygotowanieRaportu(string magSym, string grupyPodst,string grupyDowol, string dostaw, string produc, bool pominArch, bool tylkoTow)
        {
            string podstawoweGT = grupyPodst;
            string dowolneGT = grupyDowol;
            string dostawcy = dostaw;
            string producenci = produc;
            bool chPominArchiwalne = pominArch;
            bool chTylkoTowar = tylkoTow;

            string magazyny = magSym;

            DateTime dataOd = new DateTime();
            dataOd = new DateTime(2016, 10, 1);
            DateTime dataDo = new DateTime();
            
            //przygotowanie zapytania
            string sql = " ";
            sql += " select SKROT, sum(AKTUALNY_STAN) STAN_MAGAZYNU, sum(ILOSC_SPRZEDANA) SPRZEDANE_DNIA ";
            sql += " from ( ";

            sql += " select GM_TOWARY.SKROT, 0 AKTUALNY_STAN, 0 ILOSC_SPRZEDANA ";
            sql += " FROM GM_TOWARY ";
            sql += " left join R3_CONTACTS R3DOST on R3DOST.ID=GM_TOWARY.DOSTAWCA ";
            sql += " left join R3_CONTACTS R3PRODU on R3PRODU.ID=GM_TOWARY.PRODUCENT ";
            if (podstawoweGT.Length != 0)
                sql += " left join GM_GRUPYT on GM_GRUPYT.ID=GM_TOWARY.GRUPA ";
            if (dowolneGT.Length != 0)
            {
                sql += " left join GM_GRUPYT_EXT_POW on GM_GRUPYT_EXT_POW.ID_TOWARU=GM_TOWARY.ID_TOWARU ";
                sql += " left join GM_GRUPYT_EXT on GM_GRUPYT_EXT_POW.ID_GRUPY=GM_GRUPYT_EXT.ID ";
            }
            string tmpsql = "";
            if (dowolneGT.Length != 0)
            {
                if (tmpsql.Length != 0)
                    tmpsql += " and GM_GRUPYT_EXT.NAZWA in (" + dowolneGT + ")";
                else
                    tmpsql += " GM_GRUPYT_EXT.NAZWA in (" + dowolneGT + ")";
            }
            if (podstawoweGT.Length != 0)
            {
                if (tmpsql.Length != 0)
                    tmpsql += " and GM_GRUPYT.NAZWA in (" + podstawoweGT + ")";
                else
                    tmpsql += " GM_GRUPYT.NAZWA in (" + podstawoweGT + ")";
            }
            if (dostawcy.Length != 0)
            {
                if (tmpsql.Length != 0)
                    tmpsql += " and R3DOST.SHORT_NAME in (" + dostawcy + ")";
                else
                    tmpsql += " R3DOST.SHORT_NAME in (" + dostawcy + ")";
            }
            if (producenci.Length != 0)
            {
                if (tmpsql.Length != 0)
                    tmpsql += " and R3PRODU.SHORT_NAME in (" + producenci + ")";
                else
                    tmpsql += " R3PRODU.SHORT_NAME in (" + producenci + ")";
            }
            if (chPominArchiwalne)
            {
                if (tmpsql.Length != 0)
                    tmpsql += " and GM_TOWARY.ARCHIWALNY=0 ";
                else
                    tmpsql += " GM_TOWARY.ARCHIWALNY=0 ";
            }
            if (chTylkoTowar)
            {
                if (tmpsql.Length != 0)
                    tmpsql += " and GM_TOWARY.TYP='Towar' ";
                else
                    tmpsql += " GM_TOWARY.TYP='Towar' ";
            }
            if (tmpsql.Length > 0)
                sql += " where " + tmpsql;

            sql += " group by  GM_TOWARY.SKROT ";


            sql += " union ";


            sql += " select GM_TOWARY.SKROT, sum(GM_MAGAZYN.ILOSC) AKTUALNY_STAN, 0 ILOSC_SPRZEDANA ";
            sql += " FROM GM_TOWARY ";
            sql += " left join GM_MAGAZYN on GM_TOWARY.ID_TOWARU=GM_MAGAZYN.ID_TOWAR ";
            sql += " left join GM_MAGAZYNY on GM_MAGAZYNY.ID=GM_MAGAZYN.MAGNUM ";
            sql += " left join R3_CONTACTS R3DOST on R3DOST.ID=GM_TOWARY.DOSTAWCA ";
            sql += " left join R3_CONTACTS R3PRODU on R3PRODU.ID=GM_TOWARY.PRODUCENT ";
            if (podstawoweGT.Length != 0)
                sql += " left join GM_GRUPYT on GM_GRUPYT.ID=GM_TOWARY.GRUPA ";
            if (dowolneGT.Length != 0)
            {
                sql += " left join GM_GRUPYT_EXT_POW on GM_GRUPYT_EXT_POW.ID_TOWARU=GM_TOWARY.ID_TOWARU ";
                sql += " left join GM_GRUPYT_EXT on GM_GRUPYT_EXT_POW.ID_GRUPY=GM_GRUPYT_EXT.ID ";
            }

            tmpsql = "";
            if (magazyny.Length != 0)
            {
                if (tmpsql.Length != 0)
                    tmpsql += " and GM_MAGAZYNY.NUMER in (" + magazyny + ")";
                else
                    tmpsql += " GM_MAGAZYNY.NUMER in (" + magazyny + ")";
            }
            if (dowolneGT.Length != 0)
            {
                if (tmpsql.Length != 0)
                    tmpsql += " and GM_GRUPYT_EXT.NAZWA in (" + dowolneGT + ")";
                else
                    tmpsql += " GM_GRUPYT_EXT.NAZWA in (" + dowolneGT + ")";
            }
            if (podstawoweGT.Length != 0)
            {
                if (tmpsql.Length != 0)
                    tmpsql += " and GM_GRUPYT.NAZWA in (" + podstawoweGT + ")";
                else
                    tmpsql += " GM_GRUPYT.NAZWA in (" + podstawoweGT + ")";
            }
            if (dostawcy.Length != 0)
            {
                if (tmpsql.Length != 0)
                    tmpsql += " and R3DOST.SHORT_NAME in (" + dostawcy + ")";
                else
                    tmpsql += " R3DOST.SHORT_NAME in (" + dostawcy + ")";
            }
            if (producenci.Length != 0)
            {
                if (tmpsql.Length != 0)
                    tmpsql += " and R3PRODU.SHORT_NAME in (" + producenci + ")";
                else
                    tmpsql += " R3PRODU.SHORT_NAME in (" + producenci + ")";
            }
            if (tmpsql.Length > 0)
                sql += " where " + tmpsql;

            sql += " group by  GM_TOWARY.SKROT ";

            sql += " union ";

            sql += " select GM_TOWARY.SKROT, 0 AKTUALNY_STAN, sum(GM_FSPOZ.ILOSC) ILOSC_SPRZEDANA ";
            sql += " from GM_FSPOZ ";
            sql += " join GM_TOWARY on GM_TOWARY.ID_TOWARU=GM_FSPOZ.ID_TOWARU ";
            sql += " join gm_fs on gm_fspoz.id_glowki=gm_fs.id ";
            sql += " left join GM_MAGAZYNY on GM_FS.MAGNUM=GM_MAGAZYNY.ID ";
            if (dowolneGT.Length != 0)
            {
                sql += " left join GM_GRUPYT_EXT_POW on GM_GRUPYT_EXT_POW.ID_TOWARU=GM_FSPOZ.ID_TOWARU ";
                sql += " left join GM_GRUPYT_EXT on GM_GRUPYT_EXT_POW.ID_GRUPY=GM_GRUPYT_EXT.ID ";
            }
            if (podstawoweGT.Length != 0)
                sql += " left join GM_GRUPYT on GM_GRUPYT.ID=GM_TOWARY.GRUPA ";
            sql += " left join R3_CONTACTS R3DOST on R3DOST.ID=GM_TOWARY.DOSTAWCA ";
            sql += " left join R3_CONTACTS R3PRODU on R3PRODU.ID=GM_TOWARY.PRODUCENT ";
            sql += " where gm_fs.data_wystawienia>='" + dataOd.ToShortDateString() + "' and gm_fs.data_wystawienia<='" + dataDo.ToShortDateString() + "'";
            if (magazyny.Length != 0)
                sql += " and GM_MAGAZYNY.NUMER in (" + magazyny + ")";
            if (dowolneGT.Length != 0)
                sql += " and GM_GRUPYT_EXT.NAZWA in (" + dowolneGT + ")";
            if (podstawoweGT.Length != 0)
                sql += " and GM_GRUPYT.NAZWA in (" + podstawoweGT + ")";
            if (dostawcy.Length != 0)
                sql += " and R3DOST.SHORT_NAME in (" + dostawcy + ")";
            if (producenci.Length != 0)
                sql += " and R3PRODU.SHORT_NAME in (" + producenci + ")";
            sql += " group by  GM_TOWARY.SKROT ";

            sql += " ) a ";
            sql += " group by SKROT ";

            string file = "";
            if (magazyny == "'KRAK'")
                file = "N00780.csv"; //Kraków
            else if (magazyny == "'WARS'")
                file = "N04964.csv"; //Warszawa (Puławska)
            else if (magazyny == "'PRZE'")
                file = "N03885.csv"; //Przemyśl
            else if (magazyny.Contains("'NOWY'") && magazyny.Contains("'CENTR'"))
                file = "N00779.csv"; //Nowy Sącz magazyn główny i pomocniczy
            else if (magazyny == "'NOWY'")
                file = "N00779.csv"; //Nowy Sącz
            else if (magazyny == "'WESO'")
                file = "N05484.csv"; //N05484 Warszawa (Trakt Brzeski)
            else if (magazyny == "'CENTR'")
                file = "N05533.csv"; //N05533 Nowy Sącz (magazyn centrala)
            else
            {
               file = "N00000.csv";
               logowanieDoPliku("Brak nazwy pliku dla tego magazynu, na serwer ftp zostanie zapisany plik " + file, "WARNING");
            }
            StringBuilder builder = new StringBuilder();

            FbCommand cdk = new FbCommand(sql, conn);
            int licznik = 0;
            try
            {
                FbDataReader fbDataReader = cdk.ExecuteReader();
                while (fbDataReader.Read())
                {
                    //builder.AppendFormat(cell.ColumnIndex == (dataGridView1.Columns.Count - 1) ? "{0}" : "{0};", cell.Value.ToString().Replace(",", "."));
                    //builder.AppendFormat(cell.ColumnIndex == (dataGridView1.Columns.Count - 1) ? "{0}" : "{0};", cell.Value);
                    builder.Append(fbDataReader[0] + ";" + fbDataReader[1] + ";" + fbDataReader[2]);
                    builder.AppendLine();
                    licznik++;
                }
                fbDataReader.Close();
                logowanieDoPliku("Wygenerowano " + licznik + " lini", "INFO");
            }
            catch (FbException ex)
            {
                logowanieDoPliku("Błąd wykonywania zapytanie SQL dla PowerBike: " + ex.Message,"ERROR");
                throw;
            }

            string mydocpath = Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData) + @"\" + file;
            File.WriteAllText(mydocpath, builder.ToString());
            logowanieDoPliku("Zapisano plik: " + mydocpath, "INFO");

            sendFileToFTP(mydocpath, file);
        }

        public void sendFileToFTP(string filePath, string fileName)
        {
            SettingFile setFF = new SettingFile();

            //wysłanie na ftp
            logowanieDoPliku("Właściwe wysyłanie na FTP: " + filePath + "   " + fileName, "INFO");
                try
                {


                    FtpWebRequest request = (FtpWebRequest)WebRequest.Create("ftp://" + setFF.AdresFTP +  "/" + fileName);
                    request.Method = WebRequestMethods.Ftp.UploadFile;

                    //request.Credentials = new NetworkCredential("synchro.powerbikeb", "pqUUFQK6q4");
                    request.Credentials = new NetworkCredential(setFF.UserFTP, setFF.PassFTP);

                    StreamReader sourceStream = new StreamReader(filePath);
                    byte[] fileContents = Encoding.UTF8.GetBytes(sourceStream.ReadToEnd());
                    sourceStream.Close();
                    request.ContentLength = fileContents.Length;

                    Stream requestStream = request.GetRequestStream();
                    requestStream.Write(fileContents, 0, fileContents.Length);
                    requestStream.Close();

                    FtpWebResponse response = (FtpWebResponse)request.GetResponse();

                    logowanieDoPliku("Zapisano plik na FTP z raportem: " + filePath + " Status odpowiedzi serwera:" + response.StatusDescription,"INFO");

                    response.Close();
                }
                catch (Exception ex)
                {
                    logowanieDoPliku("Bład zapisu pliku na FTP: " + filePath + " z raportem:" + ex.Message, "ERROR");
                    throw;
                }

        }

        public void logowanieDoPliku(string komunikat, string typLoga)
        {
            logowanieDoPlikuLoc(komunikat, typLoga);
            string logpath = Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData) + @"\logRaksExportToFTPPowerBike" + DateTime.Now.Year + DateTime.Now.Month + DateTime.Now.Day + ".log" ;
            string logpathImex = @"C:\\imex\\imexLogRaksExportToFTPPowerBike" + DateTime.Now.Year + DateTime.Now.Month + DateTime.Now.Day + ".log";
            if (!File.Exists(logpath))
            {
                try
                {
                    using (StreamWriter sw = File.CreateText(logpath))
                    {
                        sw.WriteLine(typLoga + ";" + DateTime.Now.ToString() + ";" + komunikat);
                    }
                }
                catch (Exception fi)
                {
                    if (!File.Exists(logpathImex))
                    {
                        using (StreamWriter sw = File.CreateText(logpathImex))
                        {
                            sw.WriteLine(typLoga + ";" + DateTime.Now.ToString() + ";" + fi.Message);
                            sw.WriteLine(typLoga + ";" + DateTime.Now.ToString() + ";" + komunikat);
                        }
                    }
                    else
                    {
                        using (StreamWriter sw = File.AppendText(logpathImex))
                        {
                            sw.WriteLine(typLoga + ";" + DateTime.Now.ToString() + ";" + fi.Message);
                            sw.WriteLine(typLoga + ";" + DateTime.Now.ToString() + ";" + komunikat);
                        }

                    }
                }
            }
            else
            {
                using (StreamWriter sw = File.AppendText(logpath))
                {
                    sw.WriteLine(typLoga + ";" + DateTime.Now.ToString() + ";" + komunikat);
                }	
            }
        }

        public void logowanieDoPlikuLoc(string komunikat, string typLoga)
        {
            string logpath = @"C:\\imex\\allLogFTPPowerBike" + DateTime.Now.Year + DateTime.Now.Month + DateTime.Now.Day + ".log";
            if (!File.Exists(logpath))
            {
                using (StreamWriter sw = File.CreateText(logpath))
                {
                    sw.WriteLine(typLoga + ";" + DateTime.Now.ToString() + ";" + komunikat);
                }
            }
            else
            {
                using (StreamWriter sw = File.AppendText(logpath))
                {
                    sw.WriteLine(typLoga + ";" + DateTime.Now.ToString() + ";" + komunikat);
                }
            }
        } 
    }
}
