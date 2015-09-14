using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection.Emit;
using System.Text;
 

namespace Dapper
{
    public static partial class SimpleCRUD
    {
        public static string GetByPkSql<T>(this T t)
        {
             
            var idProps = GetIdProperties(t).ToList();

            if (!idProps.Any())
                throw new ArgumentException("Get<T> only supports an entity with a [Key] or Id property");
            if (idProps.Count() > 1)
                throw new ArgumentException("Get<T> only supports an entity with a single [Key] or Id property");

            var onlyKey = idProps.First();
            var name = GetTableName(t);
            var sb = new StringBuilder();
            sb.Append("Select ");
            //create a new empty instance of the type to get the base properties
            BuildSelect(sb, GetScaffoldableProperties((T)Activator.CreateInstance(typeof(T))).ToArray());
            sb.AppendFormat(" from {0}", name);
            var pkColumnName = GetColumnName(onlyKey);
            sb.Append(" where " + pkColumnName + " = @"+pkColumnName);

            //var dynParms = new DynamicParameters();
            //dynParms.Add("@id", id);

            //if (Debugger.IsAttached)
            //    Trace.WriteLine(String.Format("Get<{0}>: {1} with Id: {2}", currenttype, sb, id));

            return sb.ToString();
        }

        public static string GetListSql<T>(this T t, object whereConditions)
        {
            
            var idProps = GetIdProperties(t).ToList();
            if (!idProps.Any())
                throw new ArgumentException("Entity must have at least one [Key] property");

            var name = GetTableName(t);

            var sb = new StringBuilder();
            var whereprops = GetAllProperties(whereConditions).ToArray();
            sb.Append("Select ");
            //create a new empty instance of the type to get the base properties
            BuildSelect(sb, GetScaffoldableProperties((T)Activator.CreateInstance(typeof(T))).ToArray());
            sb.AppendFormat(" from {0}", name);

            if (whereprops.Any())
            {
                sb.Append(" where ");
                BuildWhere(sb, whereprops, (T)Activator.CreateInstance(typeof(T)));
            }

            if (Debugger.IsAttached)
                Trace.WriteLine(String.Format("GetList<{0}>: {1}", t, sb));

            return sb.ToString();
        }


        public static string InsertSql<T>(this T entity)
        {
            var idProps = GetIdProperties(entity).ToList();

            if (!idProps.Any())
                throw new ArgumentException("Insert<T> only supports an entity with a [Key] or Id property");
            if (idProps.Count() > 1)
                throw new ArgumentException("Insert<T> only supports an entity with a single [Key] or Id property");

           // var keyHasPredefinedValue = false;
 
            var name = GetTableName(entity);
            var sb = new StringBuilder();
            sb.AppendFormat("insert into {0}", name);
            sb.Append(" (");
            BuildInsertParameters(entity, sb);
            sb.Append(") ");
            sb.Append("values");
            sb.Append(" (");
            BuildInsertValues(entity, sb);
            sb.Append(")");

           

            if (Debugger.IsAttached)
                Trace.WriteLine(String.Format("Insert: {0}", sb));

            return sb.ToString();
        }

        public static string UpdateSql<T>(this T t)
        {
            var idProps = GetIdProperties(t).ToList();

            if (!idProps.Any())
                throw new ArgumentException("Entity must have at least one [Key] or Id property");

            var name = GetTableName(t);

            var sb = new StringBuilder();
            sb.AppendFormat("update {0}", name);

            sb.AppendFormat(" set ");
            BuildUpdateSet(t, sb);
            sb.Append(" where ");
            BuildWhere(sb, idProps, t);

            if (Debugger.IsAttached)
                Trace.WriteLine(String.Format("Update: {0}", sb));

            return sb.ToString();
            //   return connection.Execute(sb.ToString(), entityToUpdate, transaction, commandTimeout);
        }

        public static string DeleteSql<T>(this T t)
        {
            var idProps = GetIdProperties(t).ToList();


            if (!idProps.Any())
                throw new ArgumentException("Entity must have at least one [Key] or Id property");

            var name = GetTableName(t);

            var sb = new StringBuilder();
            sb.AppendFormat("delete from {0}", name);

            sb.Append(" where ");
            BuildWhere(sb, idProps, t);

            if (Debugger.IsAttached)
                Trace.WriteLine(String.Format("Delete: {0}", sb));

            // return connection.Execute(sb.ToString(), entityToDelete, transaction, commandTimeout);
            return sb.ToString();
        }

        public static string DeleteByPkSql<T>(this T t)
        {
            var currenttype = typeof(T);
            var idProps = GetIdProperties(currenttype).ToList();


            if (!idProps.Any())
                throw new ArgumentException("Delete<T> only supports an entity with a [Key] or Id property");
            if (idProps.Count() > 1)
                throw new ArgumentException("Delete<T> only supports an entity with a single [Key] or Id property");

            var onlyKey = idProps.First();
            var name = GetTableName(currenttype);

            var sb = new StringBuilder();
            sb.AppendFormat("Delete from {0}", name);

            var pkColumnName = GetColumnName(onlyKey);
            sb.Append(" where " + pkColumnName + " = @" + pkColumnName);

           // sb.Append(" where " + GetColumnName(onlyKey) + " = @Id");

            //var dynParms = new DynamicParameters();
            //dynParms.Add("@id", id);

            //if (Debugger.IsAttached)
            //    Trace.WriteLine(String.Format("Delete<{0}> {1}", currenttype, sb));

           // return connection.Execute(sb.ToString(), dynParms, transaction, commandTimeout);
            return sb.ToString();
        }


        /// <summary>
        /// <para>By default queries the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>By default filters on the Id column</para>
        /// <para>-Id column name can be overridden by adding an attribute on your primary key property [Key]</para>
        /// <para>Returns a single entity by a single id from table T</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="id"></param>
        /// <param name="trans"></param>
        /// <returns>Returns a single entity by a single id from table T.</returns>
        public static T Get<T>(this IDbConnection connection,string sql, object id, IDbTransaction trans = null)
        {
            //var currenttype = typeof(T);
            //var idProps = GetIdProperties(currenttype).ToList();

            //if (!idProps.Any())
            //    throw new ArgumentException("Get<T> only supports an entity with a [Key] or Id property");
            //if (idProps.Count() > 1)
            //    throw new ArgumentException("Get<T> only supports an entity with a single [Key] or Id property");

            //var onlyKey = idProps.First();
            //var name = GetTableName(currenttype);
            //var sb = new StringBuilder();
            //sb.Append("Select ");
            ////create a new empty instance of the type to get the base properties
            //BuildSelect(sb, GetScaffoldableProperties((T)Activator.CreateInstance(typeof(T))).ToArray());
            //sb.AppendFormat(" from {0}", name);
            //sb.Append(" where " + GetColumnName(onlyKey) + " = @Id");

            var dynParms = new DynamicParameters();
            dynParms.Add("@id", id);

            //if (Debugger.IsAttached)
            //    Trace.WriteLine(String.Format("Get<{0}>: {1} with Id: {2}", currenttype, sb, id));

            return connection.Query<T>(sql, dynParms, trans).FirstOrDefault();
        }

        /// <summary>
        /// <para>By default queries the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>whereConditions is an anonymous type to filter the results ex: new {Category = 1, SubCategory=2}</para>
        /// <para>Returns a list of entities that match where conditions</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="whereConditions"></param>
        /// <returns>Gets a list of entities with optional exact match where conditions</returns>
        public static IEnumerable<T> GetList<T>(this IDbConnection connection,string sql, object whereConditions, IDbTransaction trans = null)
        {
            var currenttype = typeof(T);
            //var idProps = GetIdProperties(currenttype).ToList();
            //if (!idProps.Any())
            //    throw new ArgumentException("Entity must have at least one [Key] property");

            //var name = GetTableName(currenttype);

            var sb = new StringBuilder();

            sb.Append(sql);

            var whereprops = GetAllProperties(whereConditions).ToArray();
            //sb.Append("Select ");
            ////create a new empty instance of the type to get the base properties
            //BuildSelect(sb, GetScaffoldableProperties((T)Activator.CreateInstance(typeof(T))).ToArray());
            //sb.AppendFormat(" from {0}", name);

            if (whereprops.Any())
            {
                sb.Append(" where ");
                BuildWhere(sb, whereprops, (T)Activator.CreateInstance(typeof(T)));
            }

            if (Debugger.IsAttached)
                Trace.WriteLine(String.Format("GetList<{0}>: {1}", currenttype, sb));

            return connection.Query<T>(sb.ToString(), whereConditions, trans);
        }


        /// <summary>
        /// <para>By default queries the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>conditions is an SQL where clause and/or order by clause ex: "where name='bob'"</para>
        /// <para>Returns a list of entities that match where conditions</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="conditions"></param>
        /// <returns>Gets a list of entities with optional SQL where conditions</returns>
        public static IEnumerable<T> GetList<T>(this IDbConnection connection,string sql, string conditions=null, IDbTransaction trans = null)
        {
            var currenttype = typeof(T);
            

            var sb = new StringBuilder();

            sb.Append(sql);

            if (!string.IsNullOrEmpty(conditions))
            {
                sb.Append(" " + conditions);
            }

            if (Debugger.IsAttached)
                Trace.WriteLine(String.Format("GetList<{0}>: {1}", currenttype, sb));

            return connection.Query<T>(sb.ToString(), transaction: trans);
        }

        ///// <summary>
        ///// <para>By default queries the table matching the class name</para>
        ///// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        ///// <para>Returns a list of all entities</para>
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="connection"></param>
        ///// <returns>Gets a list of all entities</returns>
        //public static IEnumerable<T> GetList<T>(this IDbConnection connection, string sql,IDbTransaction trans = null)
        //{
        //    return connection.GetList<T>(sql,new { }, trans);
        //}

        /// <summary>
        /// <para>By default queries the table matching the class name</para>
        /// <para>-Table name can be overridden by adding an attribute on your class [Table("YourTableName")]</para>
        /// <para>conditions is an SQL where clause ex: "where name='bob'" - not required </para>
        /// <para>orderby is a column or list of columns to order by ex: "lastname, age desc" - not required - default is by primary key</para>
        /// <para>Returns a list of entities that match where conditions</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="pageNumber"></param>
        /// <param name="rowsPerPage"></param>
        /// <param name="conditions"></param>
        /// <param name="orderby"></param>
        /// <returns>Gets a paged list of entities with optional exact match where conditions</returns>
        public static IEnumerable<T> GetListPaged<T>(this IDbConnection connection,string sql, int pageNumber, int rowsPerPage, string conditions, string orderby, IDbTransaction trans = null)
        {
            if (string.IsNullOrEmpty(_getPagedListSql))
                throw new Exception("GetListPage is not supported with the current SQL Dialect");

            var pagedListSql = _getPagedListSql.Replace(_getPagedListSelectCommon, string.Empty).Replace(_getPagedListSelectMsSql,string.Empty);


            if (pageNumber < 1)
                throw new Exception("Page must be greater than 0");

            var currenttype = typeof(T);
            var idProps = GetIdProperties(currenttype).ToList();
            if (!idProps.Any())
                throw new ArgumentException("Entity must have at least one [Key] property");
            
            var query = sql+pagedListSql;

            if (string.IsNullOrEmpty(orderby))
            {
                orderby = idProps.First().Name;
            }
            
            query = query.Replace("{PageNumber}", pageNumber.ToString());
            query = query.Replace("{RowsPerPage}", rowsPerPage.ToString());
            query = query.Replace("{OrderBy}", orderby);
            query = query.Replace("{WhereClause}", conditions);

            if (Debugger.IsAttached)
                Trace.WriteLine(String.Format("GetListPaged<{0}>: {1}", currenttype, query));

            return connection.Query<T>(query, transaction: trans);
        }
    }
}
