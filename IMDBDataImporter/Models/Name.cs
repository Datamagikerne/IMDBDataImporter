using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBDataImporter.Models
{
    public class Name
    {
        public Name(string nconst, string primaryName, int birthYear, int? deathYear)
        {
            this.nconst = nconst;
            this.primaryName = primaryName;
            this.birthYear = birthYear;
            this.deathYear = deathYear;
        }

        public string nconst { get; set; }
        public string primaryName { get; set; }
        public int birthYear { get; set; }
        public int? deathYear { get; set; }
    }
}
