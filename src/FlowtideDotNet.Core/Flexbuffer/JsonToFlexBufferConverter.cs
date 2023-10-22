// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Text;
using System.Globalization;
using System.Buffers;

// This code is based on LightJson project.
// https://github.com/MarcosLopezC/LightJson
// Big thanks goes to Marcos Vladimir López Castellanos
// https://github.com/MarcosLopezC
namespace FlexBuffers
{


    public class JsonToFlexBufferConverter
    {
	    private readonly TextScanner _scanner;
	    
	    private JsonToFlexBufferConverter(TextReader reader)
	    {
		    _scanner = new TextScanner(reader);
	    }
	    public static byte[] Convert(TextReader reader, FlexBuffer.Options options = FlexBuffer.Options.ShareKeys | FlexBuffer.Options.ShareStrings | FlexBuffer.Options.ShareKeyVectors)
	    {
		    if (reader == null)
		    {
			    throw new ArgumentNullException(nameof(reader));
		    }
			var flx = new FlexBuffer(ArrayPool<byte>.Shared, options:options);
            flx.NewObject();
            new JsonToFlexBufferConverter(reader).ReadJsonValue(flx);
			return flx.Finish();
	    }
	    
	    public static byte[] ConvertFile(string path, FlexBuffer.Options options = FlexBuffer.Options.ShareKeys | FlexBuffer.Options.ShareStrings | FlexBuffer.Options.ShareKeyVectors)
	    {
		    if (path == null)
		    {
			    throw new ArgumentNullException(nameof(path));
		    }

		    using (var reader = new StreamReader(path))
		    {
			    return Convert(reader, options);
		    }
	    }
	    
	    public static byte[] Convert(string source, FlexBuffer.Options options = FlexBuffer.Options.ShareKeys | FlexBuffer.Options.ShareStrings | FlexBuffer.Options.ShareKeyVectors)
	    {
		    if (source == null)
		    {
			    throw new ArgumentNullException(nameof(source));
		    }

		    using (var reader = new StringReader(source))
		    {
			    return Convert(reader, options);
		    }
	    }
	    
	    private void ReadJsonValue(FlexBuffer flx)
	    {
		    _scanner.SkipWhitespace();

		    var next = _scanner.Peek();

		    if (char.IsNumber(next))
		    {
			    ReadNumber(flx);
			    return;
		    }

		    switch (next)
		    {
			    case '{':
				    ReadObject(flx);
				    return;

			    case '[':
				    ReadArray(flx);
				    return;

			    case '"':
				    ReadString(flx);
				    return;

			    case '-':
				    ReadNumber(flx);
				    return;

			    case 't':
			    case 'f':
				    ReadBoolean(flx);
				    return;

			    case 'n':
				    ReadNull(flx);
				    return;

			    default:
				    throw new Exception($"Unexpected character {_scanner.Position}");
		    }
	    }
	    
	    private void ReadNull(FlexBuffer flx)
		{
			_scanner.Assert("null");
			flx.AddNull();
		}

		private void ReadBoolean(FlexBuffer flx)
		{
			switch (_scanner.Peek())
			{
				case 't':
					_scanner.Assert("true");
					flx.Add(true);
					return;

				case 'f':
					_scanner.Assert("false");
					flx.Add(false);
					return;

				default:
					throw new Exception($"Unexpected character {_scanner.Position}");
			}
		}

		private void ReadDigits(StringBuilder builder)
		{
			while (_scanner.CanRead && char.IsDigit(_scanner.Peek()))
			{
				builder.Append(_scanner.Read());
			}
		}

		private void ReadNumber(FlexBuffer flx)
		{
			var builder = new StringBuilder();

			var isFloat = false;

			if (_scanner.Peek() == '-')
			{
				builder.Append(_scanner.Read());
			}

			if (_scanner.Peek() == '0')
			{
				builder.Append(_scanner.Read());
			}
			else
			{
				ReadDigits(builder);
			}

			if (_scanner.CanRead && _scanner.Peek() == '.')
			{
				builder.Append(_scanner.Read());
				ReadDigits(builder);
				isFloat = true;
			}

			if (_scanner.CanRead && char.ToLowerInvariant(_scanner.Peek()) == 'e')
			{
				builder.Append(_scanner.Read());

				var next = _scanner.Peek();

				switch (next)
				{
					case '+':
					case '-':
						builder.Append(_scanner.Read());
						break;
				}

				ReadDigits(builder);
			}

			if (isFloat)
			{
				var value = double.Parse(
					builder.ToString(),
					CultureInfo.InvariantCulture
				);
				flx.Add(value);
			}
			else
			{
				flx.Add(long.Parse(builder.ToString()));
			}
		}

		private void ReadString(FlexBuffer flx, bool asKey = false)
		{
			var builder = new StringBuilder();

			_scanner.Assert('"');

			while (true)
			{
				var c = _scanner.Read();

				if (c == '\\')
				{
					c = _scanner.Read();

					switch (char.ToLower(c))
					{
						case '"':  // "
						case '\\': // \
						case '/':  // /
							builder.Append(c);
							break;
						case 'b':
							builder.Append('\b');
							break;
						case 'f':
							builder.Append('\f');
							break;
						case 'n':
							builder.Append('\n');
							break;
						case 'r':
							builder.Append('\r');
							break;
						case 't':
							builder.Append('\t');
							break;
						case 'u':
							builder.Append(ReadUnicodeLiteral());
							break;
						default:
							throw new Exception($"Unexpected character {_scanner.Position}");
					}
				}
				else if (c == '"')
				{
					break;
				}
				else
				{
                    builder.Append(c);
     //               if (char.IsControl(c))
					//{
					//	throw new Exception($"Unexpected character {_scanner.Position}");
					//}
					//else
					//{
					//	builder.Append(c);
					//}
				}
			}

			if (asKey)
			{
				flx.AddKey(builder.ToString());
			}
			else
			{
				flx.Add(builder.ToString());
			}
		}

		private int ReadHexDigit()
		{
			switch (char.ToUpper(_scanner.Read()))
			{
				case '0':
					return 0;

				case '1':
					return 1;

				case '2':
					return 2;

				case '3':
					return 3;

				case '4':
					return 4;

				case '5':
					return 5;

				case '6':
					return 6;

				case '7':
					return 7;

				case '8':
					return 8;

				case '9':
					return 9;

				case 'A':
					return 10;

				case 'B':
					return 11;

				case 'C':
					return 12;

				case 'D':
					return 13;

				case 'E':
					return 14;

				case 'F':
					return 15;

				default:
					throw new Exception($"Unexpected character {_scanner.Position}");
			}
		}

		private char ReadUnicodeLiteral()
		{
			int value = 0;

			value += ReadHexDigit() * 4096; // 16^3
			value += ReadHexDigit() * 256;  // 16^2
			value += ReadHexDigit() * 16;   // 16^1
			value += ReadHexDigit();        // 16^0

			return (char)value;
		}
		
		private void ReadArray(FlexBuffer flx)
		{
			_scanner.Assert('[');

			var start = flx.StartVector();

			_scanner.SkipWhitespace();

			if (_scanner.Peek() == ']')
			{
				_scanner.Read();
			}
			else
			{
				while (true)
				{
					ReadJsonValue(flx);

					_scanner.SkipWhitespace();

					var next = _scanner.Read();

					if (next == ']')
					{
						break;
					}
					
					if (next == ',')
					{
						continue;
					}
					
					throw new Exception($"Unexpected character {next} at position {_scanner.Position}");
				}
			}

			flx.EndVector(start, false, false);
		}
		
		private void ReadObject(FlexBuffer flx)
		{
			_scanner.Assert('{');

			_scanner.SkipWhitespace();

			var start = flx.StartVector();

			if (_scanner.Peek() == '}')
			{
				_scanner.Read();
			}
			else
			{
				while (true)
				{
					_scanner.SkipWhitespace();

					ReadString(flx, true);

					_scanner.SkipWhitespace();

					_scanner.Assert(':');

					_scanner.SkipWhitespace();

					ReadJsonValue(flx);

					_scanner.SkipWhitespace();

					var next = _scanner.Read();

					if (next == '}')
					{
						break;
					}
					
					if (next == ',')
					{
						continue;
					}
					throw new Exception($"Unexpected character {next} at position {_scanner.Position}");
				}
			}

			flx.SortAndEndMap(start);
		}
    }
}