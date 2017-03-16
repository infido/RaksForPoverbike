using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Topshelf;
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
            HostFactory.Run(x =>                                 //1
            {
                x.Service<Wysylacz>(s =>                        //2
                {
                    s.ConstructUsing(name => new Wysylacz());     //3
                    s.WhenStarted(tc => tc.Start());              //4
                    s.WhenStopped(tc => tc.Stop());               //5
                });
                x.RunAsLocalSystem();                            //6

                x.SetDescription("Usluga wysylania raportow do Poverbike");        //7
                x.SetDisplayName("Wysylacz do PB");                       //8
                x.SetServiceName("Wysylacz_ftp_PB");                       //9
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
            //10 sekund Timer(10000) >> jak ustawic raz na dobę o konkretnej godzinie???
            _timer = new Timer(10000) { AutoReset = true }; 
            _timer.Elapsed += (sender, eventArgs) => Console.WriteLine("Wysylacz is {0} and all is well", DateTime.Now);
            _timer.Elapsed += new ElapsedEventHandler(this.wyslanie_do_ftp);
        }
        public void Start() { _timer.Start(); }
        public void Stop() { _timer.Stop(); }

        public void wyslanie_do_ftp(object sender, EventArgs e)
        {
            Console.WriteLine("Łaczymy się z FB");
            setConnectionON(true);

            //wykonanie zapytań 
            przygotowanieRaportu("'KR'","","","'POWERBIKE'","",true,true);
            przygotowanieRaportu("'WA'", "", "", "'POWERBIKE'", "", true, true);
            //przygotowanieRaportu("'M7'", "", "", "'POWERBIKE'", "", true, true);
            przygotowanieRaportu("'NS'", "", "", "'POWERBIKE'", "", true, true);

            setConnectionOFF();
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
                throw new System.ArgumentException("Brak tworzenia połączenia do FB: " + ex.Message, "original");
                //Logg logg = new Logg(Logg.RodzajLogowania.ErrorMSG, Logg.MediumLoga.File, "1002: Błąd odczytu constr z rejestrze Windows: " + ex.Message);
                //System.Windows.Forms.MessageBox.Show("1002: Błąd odczytu constr z rejestrze Windows: " + ex.Message);
            }

            return connectionString;
        }

        private void setConnectionON(Boolean _trybTest)
        {

            conn = new FbConnection(getConnectionString());
            Console.WriteLine("9001: Ustawiono parametry połaczenia. ");

            try
            {
                conn.Open();
                if (conn.State > 0)
                {
                    if (_trybTest)
                    {
                        Console.WriteLine("9002: Nawiązano połaczenie. " + conn.Database + " Status=" + conn.State);
                    }
                    else
                    {
                        Console.WriteLine("9003: Nawiązano połaczenie! " + conn.Database + " Status=" + conn.State);
                    }
                }
                else
                {
                    if (_trybTest)
                    {
                        Console.WriteLine("1003: Nie połączono! Status=" + conn.State);
                    }
                    else
                    {
                        Console.WriteLine("1004: Błąd połączenia z bazą!");
                    }
                }
            }
            catch (Exception ex)
            {
                if (_trybTest)
                {
                    Console.WriteLine("1005: Błąd: " + ex.Message);
                }
                else
                {
                    Console.WriteLine("1006: Błąd: " + ex.Message);
                }
            }
        }

        public void setConnectionOFF()
        {
            conn.Close();
            Console.WriteLine("9003: Rozłaczono! Status=" + conn.State);
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
               Console.WriteLine("Brak nazwy pliku dla tego magazynu, na serwer ftp zostanie zapisany plik " + file);
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
                Console.WriteLine("Wygenerowano {0} lini", licznik);
            }
            catch (FbException ex)
            {
                Console.WriteLine("Błąd wykonywania zapytanie SQL dla PowerBike: " + ex.Message);
                throw;
            }

            string mydocpath = Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData) + @"\" + file;
            File.WriteAllText(mydocpath, builder.ToString());
            Console.WriteLine("Zapisano plik: "+ mydocpath);

            //sendFileToFTP(mydocpath);
        }

        public void sendFileToFTP(string filePath)
        {
            //wysłanie na ftp
                try
                {


                    FtpWebRequest request = (FtpWebRequest)WebRequest.Create("ftp://ftp.powerbikeb.ogicom.pl/" + filePath);
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

                    Console.WriteLine("Zapisano plik z raportem: " + filePath + " Status odpowiedzi serwera:" + response.StatusDescription);

                    response.Close();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Bład zapisu pliku:" + filePath + " z raportem:" + ex.Message);
                    throw;
                }

        }
    }
}
