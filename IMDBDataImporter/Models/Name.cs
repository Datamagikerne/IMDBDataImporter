using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBDataImporter.Models
{
    public class Name
    {
        public Name(string nconst, string primaryName, int? birthYear, int? deathYear, string professionsString, string titlesString)
        {
            this.nconst = nconst;
            this.primaryName = primaryName;
            this.birthYear = birthYear;
            this.deathYear = deathYear;

            primaryProfession= professionsString.Split(",").ToList();
            knownForTitles= titlesString.Split(",").ToList();
        }

        public string nconst { get; set; }
        public string primaryName { get; set; }
        public int? birthYear { get; set; }
        public int? deathYear { get; set; }
        public List<string> primaryProfession { get; set; }
        public List<string> knownForTitles { get; set; }
    }
}
