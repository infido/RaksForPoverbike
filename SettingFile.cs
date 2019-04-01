using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace RaksForPoverbike
{
    public class SettingFile
    {
        private string adresFTP, userFTP, passFTP, database, dataSourcePath;
        private string konfIniName = "konf.ini";

        public string DataSourcePath
        {
            get { return dataSourcePath; }
            set { dataSourcePath = value; }
        }

        public string Database
        {
            get { return database; }
            set { database = value; }
        }

        public string PassFTP
        {
            get { return passFTP; }
            set { passFTP = value; }
        }

        public string UserFTP
        {
            get { return userFTP; }
            set { userFTP = value; }
        }

        public string AdresFTP
        {
            get { return adresFTP; }
            set { adresFTP = value; }
        }

        public SettingFile()
        {
            wycztajUstawienia();
        }

        public SettingFile(bool sprawdzCzyPlikJest)
        {
            FileStream plik;
            StreamWriter zapisz;

            if (sprawdzCzyPlikJest)
            {
                logowanieDoPlikuLocSettings("Sprawdzenie czy pliku konfiguracji programu istnieje w lokalizacji " + Directory.GetCurrentDirectory(), "INFO");
                try
                {
                    if (File.Exists(konfIniName) == false)
                    {
                        logowanieDoPlikuLocSettings(">>>>>>> Tworze nowy pliku konfiguracji programu przy sprawdzaniu i uruchomieniu", "INFO");

                        plik = new FileStream(konfIniName, FileMode.CreateNew, FileAccess.Write);
                        zapisz = new StreamWriter(plik);

                        zapisz.WriteLine("/usr/raks/Data/F00001.fdb;");
                        zapisz.WriteLine("10.0.0.100");
                        zapisz.WriteLine("adresF");
                        zapisz.WriteLine("userF");
                        zapisz.WriteLine("haslF");

                        zapisz.Close();
                        plik.Close();
                    }
                }
                catch (Exception e)
                {
                    logowanieDoPlikuLocSettings("Bład wczytywania przy sprawdzaniu czy plik konfiguracji istnieje:" + e.Message, "ERROR");
                }
            }
            else
            {
                logowanieDoPlikuLocSettings("Sprawdzenie czy pliku konfiguracji programu istnieje, w trybie bez sprawdzania", "INFO");
            }
        }

        private void wycztajUstawienia()
        {
            logowanieDoPlikuLocSettings("Wczytywanie pliku konfiguracji programu z biblioteki", "INFO");

            FileStream plik;
            StreamReader czytaj;
            StreamWriter zapisz;

            logowanieDoPlikuLocSettings(">> Aktualny katalog pracy " + Directory.GetCurrentDirectory(), "INFO");

            try
            {
                if (File.Exists(konfIniName))
                {
                    plik = new FileStream(konfIniName, FileMode.Open, FileAccess.Read);
                    czytaj = new StreamReader(plik);

                    database = czytaj.ReadLine();
                    dataSourcePath = czytaj.ReadLine();

                    adresFTP = czytaj.ReadLine();
                    userFTP = czytaj.ReadLine();
                    passFTP = czytaj.ReadLine();
                
                    czytaj.Close();
                    plik.Close();

                    logowanieDoPlikuLocSettings("Dane konfiguracji programu wczytano poprawnie", "INFO");
                }else{
                    logowanieDoPlikuLocSettings(">>>>>>> Tworze nowy pliku konfiguracji programu", "INFO");
                    
                    plik = new FileStream(konfIniName, FileMode.CreateNew, FileAccess.Write);
                    zapisz = new StreamWriter(plik);

                    zapisz.WriteLine("/usr/raks/Data/F00001.fdb;");
                    zapisz.WriteLine("10.0.0.100");
                    zapisz.WriteLine("adresF");
                    zapisz.WriteLine("userF");
                    zapisz.WriteLine("haslF");

                    zapisz.Close();
                    plik.Close();
                }
            }
            catch (IOException e)
            {
                logowanieDoPlikuLocSettings("Bład wczytywania pliku konfiguracji polaczenia w bibliotece:" + e.Message, "ERROR");
            }
        }

        public void logowanieDoPlikuLocSettings(string komunikat, string typLoga)
        {
            string logpath = @"C:\\imex\\logFTPPowerBikeSettings" + DateTime.Now.Year + DateTime.Now.Month + DateTime.Now.Day + ".log";
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
