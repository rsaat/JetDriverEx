..\..\..\..\Tools\ilmerge\ILMerge.exe /targetplatform:v4 /out:NHibernate.JetDriverM.dll NHibernate.JetDriver.dll .\..\..\..\..\lib\net\4.0\log4net.dll .\..\..\..\..\lib\net\4.0\NHibernate.dll
ren NHibernate.JetDriver.dll NHibernate.JetDriver.NoMerged.dll
ren NHibernate.JetDriverM.dll NHibernate.JetDriver.dll