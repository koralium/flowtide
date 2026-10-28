// Licensed under the Apache License, Version 2.0.
using System.Globalization;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using YamlDotNet.Core;
using YamlDotNet.RepresentationModel;

namespace FlowtideDotNet.Substrait.Dialect.Tests;

internal static class YamlJson
{
    public static JsonObject Load(string text)
    {
        var yaml = new YamlStream();
        yaml.Load(new StringReader(text));
        if (yaml.Documents.Count != 1)
            throw new InvalidDataException("Expected exactly one YAML document.");

        var json = Convert(yaml.Documents[0].RootNode, new HashSet<YamlNode>(ReferenceEqualityComparer.Instance));
        // Normalize numeric JsonValue representations so GetValue<int>() and GetValue<long>() both work.
        return json == null ? throw new InvalidDataException("Expected a YAML mapping at the document root.")
            : JsonNode.Parse(json.ToJsonString()) as JsonObject
            ?? throw new InvalidDataException("Expected a YAML mapping at the document root.");
    }

    private static JsonNode? Convert(YamlNode node, HashSet<YamlNode> active)
    {
        if (!active.Add(node)) throw new InvalidDataException("Recursive YAML aliases are not supported.");
        try
        {
            switch (node)
            {
                case YamlMappingNode mapping:
                    var result = new JsonObject();
                    foreach (var pair in mapping.Children)
                    {
                        if (pair.Key is not YamlScalarNode { Value: { } key })
                            throw new InvalidDataException("JSON object keys must be YAML scalars.");
                        if (result.ContainsKey(key)) throw new InvalidDataException($"Duplicate YAML key '{key}'.");
                        result.Add(key, Convert(pair.Value, active));
                    }
                    return result;
                case YamlSequenceNode sequence:
                    var array = new JsonArray();
                    foreach (var item in sequence.Children) array.Add(Convert(item, active));
                    return array;
                case YamlScalarNode scalar:
                    return Scalar(scalar);
                default:
                    throw new InvalidDataException($"Unsupported YAML node {node.NodeType}.");
            }
        }
        finally { active.Remove(node); }
    }

    private static JsonNode? Scalar(YamlScalarNode scalar)
    {
        var value = scalar.Value ?? "";
        if (scalar.Tag == "tag:yaml.org,2002:str" || scalar.Style is ScalarStyle.SingleQuoted or ScalarStyle.DoubleQuoted
            or ScalarStyle.Literal or ScalarStyle.Folded)
            return JsonValue.Create(value);

        // YAML 1.2 core types: words such as ON, OFF and YES remain strings.
        if (value is "" or "~" || value.Equals("null", StringComparison.OrdinalIgnoreCase)) return null;
        if (bool.TryParse(value, out var boolean)) return JsonValue.Create(boolean);
        if (Regex.IsMatch(value, @"^[-+]?[0-9]+$"))
        {
            if (long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var integer))
                return JsonValue.Create(integer);
            if (decimal.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var largeInteger))
                return JsonValue.Create(largeInteger);
        }
        if (Regex.IsMatch(value, @"^[-+]?(?:[0-9]+\.[0-9]*|[0-9]*\.[0-9]+|[0-9]+[eE][-+]?[0-9]+)(?:[eE][-+]?[0-9]+)?$")
            && decimal.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var number))
            return JsonValue.Create(number);
        return JsonValue.Create(value);
    }
}
