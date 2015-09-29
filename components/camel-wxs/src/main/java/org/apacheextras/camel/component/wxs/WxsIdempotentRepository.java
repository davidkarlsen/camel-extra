package org.apacheextras.camel.component.wxs;

import com.ibm.websphere.objectgrid.DuplicateKeyException;
import com.ibm.websphere.objectgrid.ObjectGrid;
import com.ibm.websphere.objectgrid.ObjectGridException;
import com.ibm.websphere.objectgrid.ObjectMap;
import org.apache.camel.Exchange;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.RuntimeExchangeException;
import org.apache.camel.spi.ExchangeIdempotentRepository;
import org.apache.camel.util.ObjectHelper;

/**
 * ExchangeIdempotentRepository implementation based on
 * <a href="http://www-01.ibm.com/support/knowledgecenter/SSTVLU_8.6.0/">IBM WebSphere Extreme Scale</a>
 *
 * <pre>
 * ObjectGrid and mapName to use can either be set through constructor, properties
 * or found on the exchange passed in, under the headerKeys defined by the constants: OBJECT_GRID_HEADER_NAME and OBJECT_GRID_MAP_HEADER_NAME
 * </pre>
 *
 * @see ExchangeIdempotentRepository
 * @author <a href="mailto:david@davidkarlsen.com">David J. M. Karlsen</a>
 * @since 7/28/15
 */
public class WxsIdempotentRepository<E>
    implements ExchangeIdempotentRepository<E>
{
    public static final String OBJECT_GRID_HEADER_NAME = "CamelWxsObjectGrid";

    public static final String OBJECT_GRID_MAP_HEADER_NAME = "CamelWxsObjectMapName";

    private ObjectGrid objectGrid;

    private String mapName;

    /**
     *
     * @param objectGrid
     * @param mapName
     */
    public WxsIdempotentRepository( ObjectGrid objectGrid, String mapName )
    {
        setObjectGrid( objectGrid );
        setMapName( mapName );
    }

    public WxsIdempotentRepository()
    {
    }

    /**
     *
     * @param mapName the mapName, not null or empty
     */
    public void setMapName( String mapName )
    {
        this.mapName = ObjectHelper.notEmpty( mapName, null );
    }

    /**
     *
     * @param objectGrid the grid to use, not null.
     */
    public void setObjectGrid( ObjectGrid objectGrid )
    {
        this.objectGrid = ObjectHelper.notNull( objectGrid, null );
    }

    private ObjectMap getObjectMap( Exchange exchange )
    {
        ObjectGrid objectGridToUse = objectGrid;
        String mapNameToUse = mapName;
        if ( exchange != null )
        {
            objectGridToUse = exchange.getIn().getHeader( OBJECT_GRID_HEADER_NAME, objectGrid, ObjectGrid.class );
            mapNameToUse = exchange.getIn().getHeader( OBJECT_GRID_MAP_HEADER_NAME, mapName, String.class );
        }

        try
        {
            return objectGridToUse.getSession().getMap( mapNameToUse );
        }
        catch ( ObjectGridException e )
        {
            throw new RuntimeExchangeException( e.getMessage(), exchange, e );
        }
    }


    public boolean add( final E key )
    {
        return add( null, key );
    }

    public boolean contains( E key )
    {
        return contains( null, key );
    }

    public boolean remove( E key )
    {
        return remove( null, key );
    }

    public boolean confirm( E key )
    {
        return confirm( null, key );
    }

    public void start()
        throws Exception
    {
        if ( mapName == null )
        {
            mapName = getClass().getName();
        }
    }

    public void stop()
        throws Exception
    {

    }


    @Override
    public boolean add( Exchange exchange, E key )
    {
        try
        {
            getObjectMap( exchange ).insert( key, null );
            return true;
        }
        catch ( DuplicateKeyException e )
        {
            return false;
        }
        catch ( ObjectGridException e )
        {
            throw new RuntimeCamelException( e );
        }
    }

    @Override
    public boolean contains( Exchange exchange, E key )
    {
        try
        {
            return getObjectMap( exchange ).containsKey( key );
        }
        catch ( ObjectGridException e )
        {
            throw new RuntimeCamelException( e );
        }
    }

    @Override
    public boolean remove( Exchange exchange, E key )
    {
        try
        {
            return getObjectMap( exchange ).remove( key ) != null;
        }
        catch ( ObjectGridException e )
        {
            throw new RuntimeCamelException( e );
        }
    }

    @Override
    public boolean confirm( Exchange exchange, E key )
    {
        return contains( exchange, key );
    }


}
