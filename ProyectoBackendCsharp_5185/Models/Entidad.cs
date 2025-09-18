#nullable enable
using System.Collections.Generic;

public class Entidad
{
    private Dictionary<string, object?> propiedades;

    public Entidad()
    {
        propiedades = new Dictionary<string, object?>();
    }

    public Entidad(Dictionary<string, object?> initialProperties)
    {
        propiedades = initialProperties ?? new Dictionary<string, object?>();
    }

    public object? this[string nombre]
    {
        get
        {
            if (propiedades.TryGetValue(nombre, out var value))
            {
                return value;
            }
            return null;
        }
        set
        {
            propiedades[nombre] = value;
        }
    }

    public Dictionary<string, object?> ObtenerPropiedades()
    {
        return new Dictionary<string, object?>(propiedades);
    }
}
