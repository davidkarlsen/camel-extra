package org.apacheextras.camel.component.wxs;

import com.ibm.websphere.objectgrid.DuplicateKeyException;
import com.ibm.websphere.objectgrid.ObjectGrid;
import com.ibm.websphere.objectgrid.ObjectGridException;
import com.ibm.websphere.objectgrid.ObjectMap;
import com.ibm.websphere.objectgrid.Session;
import org.apache.camel.Exchange;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * @author et2448
 * @since 9/28/15
 */
@RunWith( MockitoJUnitRunner.class )
public class WxsIdempotentRepositoryTest
    extends CamelTestSupport
{

    @Mock
    private ObjectGrid objectGrid;

    @Mock
    private Session session;

    @Mock
    private ObjectMap objectMapInjected;

    @Mock
    private ObjectMap objectMapWhenFromHeader;

    private WxsIdempotentRepository<? super Object> wxsIdempotentRepository;

    private final String someKey = "someKey";

    private final String mapNameWhenFromHeader = "mapForHeader";

    @Before
    public void before()
        throws Exception
    {
        String mapName = "someMap";
        this.wxsIdempotentRepository = new WxsIdempotentRepository<Object>( objectGrid, mapName );
        Mockito.when( objectGrid.getSession() ).thenReturn( session );
        Mockito.when( session.getMap( Mockito.eq( mapName ) ) ).thenReturn( objectMapInjected );
        Mockito.when( session.getMap( Mockito.eq( mapNameWhenFromHeader ) ) ).thenReturn( objectMapWhenFromHeader );
    }

    @Test
    public void testAddNonExisting()
    {
        assertTrue( wxsIdempotentRepository.add( someKey ) );
    }

    @Test
    public void testAddExisting()
        throws ObjectGridException
    {
        Mockito.doThrow( DuplicateKeyException.class ).when( objectMapInjected ).insert( Mockito.eq( someKey ),
                                                                                         Mockito.any() );
        assertFalse( wxsIdempotentRepository.add( someKey ) );
    }

    @Test
    public void testRemoveNonExisting()
    {
        assertFalse( wxsIdempotentRepository.remove( someKey ) );
    }

    @Test
    public void testRemoveExisting()
        throws ObjectGridException
    {
        Mockito.when( objectMapInjected.remove( Mockito.eq( someKey ) ) ).thenReturn( someKey );
        assertTrue( wxsIdempotentRepository.remove( someKey ) );
    }

    @Test
    public void testContainsConfirmExisting()
        throws ObjectGridException
    {
        Mockito.when( objectMapInjected.containsKey( Mockito.eq( someKey ) ) ).thenReturn( true );
        assertTrue( wxsIdempotentRepository.contains( someKey ) );
        assertTrue( wxsIdempotentRepository.confirm( someKey ) );
    }

    @Test
    public void testContainsConfirmNonExisting()
    {
        assertFalse( wxsIdempotentRepository.contains( someKey ) );
        assertFalse( wxsIdempotentRepository.confirm( someKey ) );
    }

    // EXCHANGE BASED TESTS BELOW THIS POINT

    private Exchange createExchange()
    {
        Exchange exchange = createExchangeWithBody( null );
        exchange.getIn().setHeader( WxsIdempotentRepository.OBJECT_GRID_HEADER_NAME, objectGrid );
        exchange.getIn().setHeader( WxsIdempotentRepository.OBJECT_GRID_MAP_HEADER_NAME, mapNameWhenFromHeader );

        return exchange;
    }

    @Test
    public void testAddWithExchangeNonExisting()
    {
        assertTrue( wxsIdempotentRepository.add( createExchange(), someKey ) );
    }

    @Test
    public void testAddWithExchangeExisting()
        throws ObjectGridException
    {
        Mockito.doThrow( DuplicateKeyException.class ).when( objectMapWhenFromHeader ).insert( Mockito.eq( someKey ),
                                                                                               Mockito.any() );
        assertFalse( wxsIdempotentRepository.add( createExchange(), someKey ) );
    }

    @Test
    public void testRemoveNonExistingWithExchange()
    {
        Exchange exchange = createExchange();
        assertFalse( wxsIdempotentRepository.remove( exchange, someKey ) );
        assertFalse( wxsIdempotentRepository.contains( exchange, someKey ) );
        assertFalse( wxsIdempotentRepository.confirm( exchange, someKey ) );
    }

    @Test
    public void testRemoveExistingWithExchange()
        throws ObjectGridException
    {
        Mockito.when( objectMapWhenFromHeader.remove( Mockito.eq( someKey ) ) ).thenReturn( someKey );

        Exchange exchange = createExchange();
        assertTrue( wxsIdempotentRepository.remove( exchange, someKey ) );
    }

    @Test
    public void testContainsConfirmExistingWithExchange()
        throws ObjectGridException
    {
        Mockito.when( objectMapWhenFromHeader.containsKey( Mockito.eq( someKey ) ) ).thenReturn( true );
        Exchange exchange = createExchange();

        assertTrue( wxsIdempotentRepository.contains( exchange, someKey ) );
        assertTrue( wxsIdempotentRepository.confirm( exchange, someKey ) );
    }

    @Test
    public void testContainsConfirmNonExistingWithExchange()
        throws ObjectGridException
    {
        Mockito.when( objectMapWhenFromHeader.containsKey( Mockito.eq( someKey ) ) ).thenReturn( false );
        Exchange exchange = createExchange();

        assertFalse( wxsIdempotentRepository.contains( exchange, someKey ) );
        assertFalse( wxsIdempotentRepository.confirm( exchange, someKey ) );
    }


}
