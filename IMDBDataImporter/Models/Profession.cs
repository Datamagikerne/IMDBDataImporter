using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBDataImporter.Models
{
    public class Profession
    {
        public Profession(int profession_id, string professionName)
        {
            this.profession_id = profession_id;
            this.professionName = professionName;
        }

        public int profession_id { get; set; }
        public string professionName { get; set; }
    }
}
