using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using NUnit.Framework.SyntaxHelpers;

namespace NHibernate.JetDriver.Tests
{
     [TestFixture]
    public class CanInstatiateObjects
    {
        [Test]
       public void CanInstatiateJetDialectEx()
        {
            var jdex = new JetDialectEx();

            var type2 = System.Type.GetType("NHibernate.JetDriver.JetDialectEx, NHibernate.JetDriver");
            var test2 = Activator.CreateInstance(type2);

        }

    }
}
