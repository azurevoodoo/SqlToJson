using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace SqlToJson
{
    public class QueryStream : Stream
    {
        private readonly SqlConnection dbConnection;
        private readonly SqlCommand command;
        private readonly SqlDataReader reader;
        private readonly ReadOnlyCollection<(string Name, int Ordinal)> columns;

        private long position;
        private bool header;
        private bool first;
        private bool last;

        private int rowPosition;
        private byte[] row;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => -1;
        public override long Position
        {
            get => position;
            set => throw new NotImplementedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (last)
            {
                return 0;
            }

            if (header)
            {
                position = 0;
                header = false;
                buffer[0] = 91; // [
                buffer[1] = 10; // LF
                return 2;
            }

            if (rowPosition >= row.Length)
            {
                if (!reader.Read())
                {
                    last = true;
                    buffer[0] = 10; // LF
                    buffer[1] = 93; // ]
                    return 2;
                }

                rowPosition = 0;
                row = JsonSerializer.SerializeToUtf8Bytes(
                    columns
                        .Where(col => !reader.IsDBNull(col.Ordinal))
                        .ToDictionary(
                            key => key.Name,
                            value => reader.GetValue(value.Ordinal))
                );
            }

            var index = 0;
            if (first)
            {
                first = false;
            }
            else if(rowPosition == 0)
            {
                buffer[0] = 44; // ,
                buffer[1] = 10; // LF
                index = 2;
            }

            for (; index < count && row.Length > rowPosition; index++, rowPosition++)
            {
                buffer[index] = row[rowPosition];
            }

            position += index;

            return index;
        }

        public override void Flush() => throw new NotImplementedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotImplementedException();
        public override void SetLength(long value) => throw new NotImplementedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotImplementedException();

        protected override void Dispose(bool disposing)
        {
            using (reader) { }
            using (command) { }
            using (dbConnection) { }
            row = null;
        }

        public static async Task<QueryStream> OpenStream(string query, string connectionString)
        {
            if (query == null)
            {
                throw new ArgumentNullException(nameof(query));
            }

            if (connectionString == null)
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            var conn = new SqlConnection(connectionString);
            await conn.OpenAsync();

            var command = conn.CreateCommand();
            command.CommandText = query;
            command.CommandTimeout = 180;

            var reader = await command.ExecuteReaderAsync();

            var columns = new ReadOnlyCollection<(string Name, int Ordinal)>(
                Enumerable.Range(0, reader.FieldCount)
                    .Select(ordinal => (reader.GetName(ordinal), ordinal))
                    .ToList()
            );

            return new QueryStream(conn, command, reader, columns);
        }

        private QueryStream(
            SqlConnection dbConnection,
            SqlCommand command,
            SqlDataReader reader,
            ReadOnlyCollection<(string Name, int Ordinal)> columns)
        {
            this.dbConnection = dbConnection ?? throw new ArgumentNullException(nameof(dbConnection));
            this.command = command ?? throw new ArgumentNullException(nameof(command));
            this.reader = reader ?? throw new ArgumentNullException(nameof(reader));
            this.columns = columns ?? throw new ArgumentNullException(nameof(columns));
            position = -1;
            header = true;
            first = true;
            row = Array.Empty<byte>();
            rowPosition = 0;
        }
    }
}