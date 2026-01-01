import asyncio
import sys
import argparse
from decimal import Decimal
import dotenv

from strategy.variational_arb import VariationalArb


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Cross-Exchange Arbitrage Bot Entry Point',
        formatter_class=argparse.RawDescriptionHelpFormatter
        )

    parser.add_argument('--exchange', type=str, default='variational',
                        help='Exchange to use (variational)')
    parser.add_argument('--ticker', type=str, default='BTC',
                        help='Ticker symbol (default: BTC)')
    parser.add_argument('--size', type=str, required=True,
                        help='Number of tokens to buy/sell per order')
    parser.add_argument('--fill-timeout', type=int, default=5,
                        help='Timeout in seconds for maker order fills (default: 5)')
    parser.add_argument('--max-position', type=Decimal, default=Decimal('0'),
                        help='Maximum position to hold (default: 0)')
    parser.add_argument('--long-threshold', type=Decimal, default=Decimal('10'),
                        help='Long threshold for Variational (default: 10)')
    parser.add_argument('--short-threshold', type=Decimal, default=Decimal('10'),
                        help='Short threshold for Variational (default: 10)')
    return parser.parse_args()


def validate_exchange(exchange):
    """Validate that the exchange is supported."""
    supported_exchanges = ['variational']
    if exchange.lower() not in supported_exchanges:
        print(f"Error: Unsupported exchange '{exchange}'")
        print(f"Supported exchanges: {', '.join(supported_exchanges)}")
        sys.exit(1)


async def main():
    """Main entry point that creates and runs the cross-exchange arbitrage bot."""
    args = parse_arguments()

    dotenv.load_dotenv()

    # Validate exchange
    validate_exchange(args.exchange)

    try:
        bot = VariationalArb(
            ticker=args.ticker.upper(),
            order_quantity=Decimal(args.size),
            fill_timeout=args.fill_timeout,
            max_position=args.max_position,
            long_ex_threshold=Decimal(args.long_threshold),
            short_ex_threshold=Decimal(args.short_threshold)
        )

        # Run the bot
        await bot.run()

    except KeyboardInterrupt:
        print("\nCross-Exchange Arbitrage interrupted by user")
        return 1
    except Exception as e:
        print(f"Error running cross-exchange arbitrage: {e}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
