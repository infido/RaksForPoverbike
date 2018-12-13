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
            HostFactory.Run(x =>                                
            {
                x.Service<Wysylacz>(s =>                        
                {
                    s.ConstructUsing(name => new Wysylacz());     
                    s.WhenStarted(tc => tc.Start());             
                    s.WhenStopped(tc => tc.Stop());               
                });
                x.RunAsLocalSystem();                           

                x.SetDescription("Usługa atumatycznego wysyłania raportów do Powerbike");        
                x.SetDisplayName("Wysyłacz na FTP do PB");                       
                x.SetServiceName("Wysylacz_ftp_PB");
            });
  
            
        }
    }

    public class Wysylacz
    {
        const string RegistryKey = "SOFTWARE\\Infido\\KonektorSQL";
        public static FbConnection conn;
        private string pathInfo;

        readonly Timer _timer;
        public Wysylacz()
        {
            //1 sekunda Timer(1000) 
            _timer = new Timer(1000 * 60 * 60) { AutoReset = true };
            _timer.Elapsed += (sender, eventArgs) => Console.WriteLine("Wysylacz is {0} and all is well", DateTime.Now);
            _timer.Elapsed += new ElapsedEventHandler(this.wyslanie_do_ftp);

        }
        public void Start() { _timer.Start(); }
        public void Stop() { _timer.Stop(); }

        public void wyslanie_do_ftp(object sender, EventArgs e)
        {
            if (DateTime.Now.Hour >= 23 && DateTime.Now.Hour < 24)
            {
                logowanieDoPliku("Łaczymy się z FB", "INFO");
                setConnectionON(true);

                //wykonanie zapytań 
                przygotowanieRaportu("'KR'", "", "", "'POWERBIKE'", "", true, true);
                przygotowanieRaportu("'WA'", "", "", "'POWERBIKE'", "", true, true);
                ////przygotowanieRaportu("'M7'", "", "", "'POWERBIKE'", "", true, true);
                przygotowanieRaportu("'NS'", "", "", "'POWERBIKE'", "", true, true);

                setConnectionOFF();
            }
            else
            {
                logowanieDoPliku("Negatywna kontrola czasu, nie wykonanie operacji", "CHECK");
            }
        }

        public string getConnectionString()
        {
            RegistryKey rejestr;
            string connectionString = "";
            try
            {
                rejestr = Registry.CurrentUser.OpenSubKey(RegistryKey);
                if (rejestr == null)
                {
                    logowanieDoPliku("Brak ustawień połaczenia z FB w rejestrze Windows","WARNING");
                    throw new System.ArgumentException("Brak ustawień połaczenia w rejestrze Windows", "original");
                }

                connectionString =
                    "User=" + (String)rejestr.GetValue("User") + ";" +
                    "Password=" + (String)rejestr.GetValue("Pass") + ";" +
                    "Database=" + (String)rejestr.GetValue("Path") + ";" +
                    "Datasource=" + (String)rejestr.GetValue("IP") + ";" +
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

                pathInfo = (String)rejestr.GetValue("IP") + ":" + (String)rejestr.GetValue("Path");
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

            conn = new FbConnection(getConnectionString());
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
            if (magazyny == "'KR'")
                file = "N00780.csv"; //Kraków
            else if (magazyny == "'WA'")
                file = "N04964.csv"; //Warszawa
            else if (magazyny == "'M7'")
                file = "N03885.csv"; //Przemyśl
            else if (magazyny == "'NS'")
                file = "N00779.csv"; //Nowy Sącz
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
            //wysłanie na ftp
                try
                {


                    FtpWebRequest request = (FtpWebRequest)WebRequest.Create("ftp://ftp.powerbikeb.ogicom.pl/" + fileName);
                    request.Method = WebRequestMethods.Ftp.UploadFile;

                    request.Credentials = new NetworkCredential("synchro.powerbikeb", "pqUUFQK6q4");

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
            string logpath = @"C:\\imex\\logRaksExportToFTPPowerBike" + DateTime.Now.Year + DateTime.Now.Month + DateTime.Now.Day + ".log";
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
